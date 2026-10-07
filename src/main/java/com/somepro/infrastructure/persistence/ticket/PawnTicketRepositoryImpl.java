package com.somepro.infrastructure.persistence.ticket;

import cn.hutool.core.util.IdUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.pagehelper.PageHelper;
import com.somepro.common.exception.BizException;
import com.somepro.domain.collateral.model.Category;
import com.somepro.domain.collateral.model.CollateralStatus;
import com.somepro.domain.pawner.model.PawnerStatus;
import com.somepro.domain.shared.model.PageResult;
import com.somepro.domain.ticket.model.PawnTicket;
import com.somepro.domain.ticket.model.PawnTicketQuery;
import com.somepro.domain.ticket.model.TicketStatus;
import com.somepro.domain.ticket.repository.PawnTicketRepository;
import com.somepro.infrastructure.config.ReactiveOperatorContext;
import com.somepro.infrastructure.persistence.audit.AuditContextHolder;
import com.somepro.infrastructure.persistence.ticket.converter.PawnTicketPoConverter;
import com.somepro.infrastructure.persistence.ticket.po.PawnTicketPO;
import com.somepro.infrastructure.persistence.ticket.po.TicketCollateralPO;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 当票仓储适配器（基础设施层）：MyBatis-Plus 阻塞 JDBC 经 blocking(...) 桥接进响应式链路。
 *
 * 本类三处关键业务语义：
 *
 * 1. 一物一票（含并发）
 *    一件当物同一时刻只准挂一张没结清（ACTIVE）的票。开票先抢 MySQL 命名锁
 *    GET_LOCK('pawn_ticket:write')（全实例互斥），锁内点该当物的在当票数再写入 ——
 *    两个人前后脚拿同一件东西来开票，也只落得了一张，后到者收到的是业务提示而不是底层错。
 *
 * 2. 票号生成 DP-yyyy-NNNN
 *    同一把写锁内：取当年票号的最大整数序号 +1（序号在 Java 侧解析，
 *    避免字符串排序把 9999 排在 10000 前），锁内算号天然不撞；
 *    取号刻意包含已撤销/已删除的票：票号一经分配永久占用。
 *    uk_ticket_no 唯一索引是最后防线，极端瞬态冲突整段重试，不甩底层错给柜台。
 *
 * 3. 票物状态联动同一事务
 *    开票把当物置「已典当」、撤销把当物回「在库」，与当票写入在同一事务里落库，
 *    票和物的状态要么一起成、要么一起回滚，不会出现「票开着、物在库」的裂账。
 *
 * 4. 冻结当户挡开票
 *    与「一物一票」同一把写锁、同一事务内点当户状态：不是正常（冻结/注销/已销户）
 *    一律不开票。冻结与开票并发时，锁内这一读把开票挡在冻结生效之后，
 *    柜台给冻结户开票收到的是业务提示而不是开出一张不该开的票。
 *    赎当、续当不在这里拦（赎当照常；续当在续当仓储里拦）。
 *
 * 锁的连接与时序同当户/当物模块：用一条【独立于事务的原始连接】在事务开启前 GET_LOCK、
 * 在事务【提交之后】才 RELEASE_LOCK，避免「锁已放、事务未提交」导致后到者算重号、漏看在当票。
 */
@Repository
public class PawnTicketRepositoryImpl implements PawnTicketRepository {

    /** 业务日期统一按行里所在时区算，避免容器 UTC 下票号跨年。 */
    private static final ZoneId BIZ_ZONE = ZoneId.of("Asia/Shanghai");
    /** 当票开立临界区命名锁（MySQL 全实例同名互斥）。 */
    private static final String WRITE_LOCK = "pawn_ticket:write";
    private static final int LOCK_WAIT_SECONDS = 10;
    private static final int MAX_RETRY = 5;

    private final PawnTicketMapper pawnTicketMapper;
    private final TicketCollateralMapper ticketCollateralMapper;
    private final TicketPawnerQueryMapper ticketPawnerQueryMapper;
    private final TransactionTemplate transactionTemplate;
    private final DataSource dataSource;

    public PawnTicketRepositoryImpl(PawnTicketMapper pawnTicketMapper,
                                    TicketCollateralMapper ticketCollateralMapper,
                                    TicketPawnerQueryMapper ticketPawnerQueryMapper,
                                    PlatformTransactionManager transactionManager,
                                    DataSource dataSource) {
        this.pawnTicketMapper = pawnTicketMapper;
        this.ticketCollateralMapper = ticketCollateralMapper;
        this.ticketPawnerQueryMapper = ticketPawnerQueryMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.dataSource = dataSource;
    }

    @Override
    public Mono<PawnTicket> insert(PawnTicket ticket) {
        return blocking(() -> {
            // 每轮重试用独立连接重新抢锁；兜住票号撞号 / 锁等待超时等瞬态冲突
            for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
                try {
                    return inWriteLock(() -> transactionTemplate.execute(status -> {
                        // 冻结当户挡开票：锁内点当户状态，不是正常一律不开（赎当照常，不走这里）
                        requireNormalPawner(ticket.getPawnerId());
                        // 一物一票：锁内点在当票，同一件东西前后脚来两张也只落得了一张
                        if (pawnTicketMapper.countActiveByCollateral(ticket.getCollateralId()) > 0) {
                            throw new BizException("该当物已有一张在当的当票，一票未结不能再开；请先结清原票");
                        }
                        PawnTicketPO po = PawnTicketPoConverter.toPo(ticket);
                        po.setId(IdUtil.getSnowflakeNextId());
                        po.setTicketNo(nextTicketNo());
                        pawnTicketMapper.insert(po);
                        // 票物联动：开票即已典当，同事务落库
                        markCollateral(ticket.getCollateralId(), CollateralStatus.PAWNED, null);
                        return PawnTicketPoConverter.toDomain(po);
                    }));
                } catch (DuplicateKeyException | TransientDataAccessException e) {
                    // uk_ticket_no 是最后防线，锁内正常不会撞；撞了整段重新取号重试
                    if (attempt == MAX_RETRY - 1) {
                        throw new BizException("系统繁忙，请稍后重试");
                    }
                    try {
                        Thread.sleep(10L * (attempt + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new BizException("系统繁忙，请稍后重试");
                    }
                }
            }
            throw new BizException("系统繁忙，请稍后重试");
        });
    }

    @Override
    public Mono<PawnTicket> update(PawnTicket ticket) {
        return blocking(() -> {
            PawnTicketPO existing = pawnTicketMapper.selectById(ticket.getId());
            if (existing == null) {
                throw new BizException("当票不存在");
            }
            // 票号、当户、当物、类别、估值与利率费率快照、状态永不改；把原值带回，避免被覆盖
            ticket.setTicketNo(existing.getTicketNo());
            ticket.setPawnerId(existing.getPawnerId());
            ticket.setCollateralId(existing.getCollateralId());
            ticket.setCategory(Category.valueOf(existing.getCategory()));
            ticket.setAppraisedValue(existing.getAppraisedValue());
            ticket.setMonthlyRate(existing.getMonthlyRate());
            ticket.setServiceRate(existing.getServiceRate());
            ticket.setStatus(TicketStatus.valueOf(existing.getStatus()));
            PawnTicketPO po = PawnTicketPoConverter.toPo(ticket);
            int rows = pawnTicketMapper.updateById(po);
            if (rows == 0) {
                throw new BizException("当票不存在");
            }
            PawnTicketPO refreshed = pawnTicketMapper.selectById(ticket.getId());
            return PawnTicketPoConverter.toDomain(Objects.requireNonNullElse(refreshed, po));
        });
    }

    @Override
    public Mono<PawnTicket> cancel(PawnTicket ticket) {
        return blocking(() -> transactionTemplate.execute(status -> {
            // 条件更新「在当 → 已撤销」：并发重复撤销只成一次，后到者拿到明确业务提示
            PawnTicketPO update = new PawnTicketPO();
            update.setStatus(TicketStatus.CANCELLED.code());
            int rows = pawnTicketMapper.update(update, Wrappers.<PawnTicketPO>lambdaUpdate()
                    .eq(PawnTicketPO::getId, ticket.getId())
                    .eq(PawnTicketPO::getStatus, TicketStatus.ACTIVE.code()));
            if (rows == 0) {
                throw new BizException("当票状态已变化，撤销未生效；请刷新后按最新状态办理");
            }
            // 票物联动：票撤了，当物回在库（只翻「已典当」的，别踩了别的流程置的状态）
            markCollateral(ticket.getCollateralId(), CollateralStatus.IN_STOCK, CollateralStatus.PAWNED);
            PawnTicketPO refreshed = pawnTicketMapper.selectById(ticket.getId());
            return PawnTicketPoConverter.toDomain(Objects.requireNonNull(refreshed, "当票不存在"));
        }));
    }

    @Override
    public Mono<PawnTicket> findById(Long id) {
        return blocking(() -> {
            PawnTicketPO po = pawnTicketMapper.selectById(id);
            return po == null ? null : PawnTicketPoConverter.toDomain(po);
        });
    }

    @Override
    public Mono<PawnTicket> findByTicketNo(String ticketNo) {
        return blocking(() -> {
            PawnTicketPO po = pawnTicketMapper.selectOne(
                    Wrappers.<PawnTicketPO>lambdaQuery().eq(PawnTicketPO::getTicketNo, ticketNo));
            return po == null ? null : PawnTicketPoConverter.toDomain(po);
        });
    }

    @Override
    public Mono<PageResult<PawnTicket>> page(int pageNum, int pageSize, PawnTicketQuery query) {
        return this.<PageResult<PawnTicket>>blocking(() -> {
            try {
                PageHelper.startPage(pageNum, pageSize);
                LambdaQueryWrapper<PawnTicketPO> wrapper = Wrappers.<PawnTicketPO>lambdaQuery();
                if (query.pawnerId() != null) {
                    wrapper.eq(PawnTicketPO::getPawnerId, query.pawnerId());
                }
                if (query.category() != null) {
                    wrapper.eq(PawnTicketPO::getCategory, query.category().code());
                }
                if (query.status() != null) {
                    wrapper.eq(PawnTicketPO::getStatus, query.status().code());
                }
                // 稳定排序：两页之间不会出现同一张票，分页结果可重复对号
                wrapper.orderByAsc(PawnTicketPO::getId);
                List<PawnTicketPO> rows = pawnTicketMapper.selectList(wrapper);
                long total = rows instanceof com.github.pagehelper.Page
                        ? ((com.github.pagehelper.Page<?>) rows).getTotal()
                        : rows.size();
                List<PawnTicket> content = rows.stream()
                        .map(PawnTicketPoConverter::toDomain)
                        .collect(Collectors.toList());
                return new PageResult<>(content, total, pageNum, pageSize);
            } finally {
                // PageHelper 靠 ThreadLocal 传分页参数，必须清，避免污染线程池下一次调用
                PageHelper.clearPage();
            }
        });
    }

    /**
     * 冻结/注销/已销户的当户不能开新当票：只在写锁事务内调用，与「一物一票」同一临界区。
     * 状态口径与当户档案一致（NORMAL/FROZEN/CLOSED），不另造值。
     */
    private void requireNormalPawner(Long pawnerId) {
        String pawnerStatus = ticketPawnerQueryMapper.findStatusByPawnerId(pawnerId);
        if (PawnerStatus.NORMAL.code().equals(pawnerStatus)) {
            return;
        }
        if (PawnerStatus.FROZEN.code().equals(pawnerStatus)) {
            throw new BizException("该当户已被冻结，名下不能再开新当票；如需办理请先解冻");
        }
        if (PawnerStatus.CLOSED.code().equals(pawnerStatus)) {
            throw new BizException("该当户已注销，不能开新当票");
        }
        throw new BizException("当户不存在或已销户，不能开新当票");
    }

    /**
     * 生成 DP-年份-序号：序号是当年已有票号（含已撤销/已删除）最大整数 +1，至少 4 位、超出自然进位。
     * 只在写锁（{@link #inWriteLock}）内调用，锁内串行所以不会撞号；
     * ticket_no 唯一索引是最后防线，极端瞬态冲突由外层整段重试兜底。
     */
    private String nextTicketNo() {
        int year = LocalDate.now(BIZ_ZONE).getYear();
        String prefix = "DP-" + year + "-";
        long maxSeq = 0L;
        for (String no : pawnTicketMapper.findTicketNosByPrefix(prefix + "%")) {
            if (no == null || !no.startsWith(prefix)) {
                continue;
            }
            String tail = no.substring(prefix.length());
            if (tail.chars().allMatch(Character::isDigit)) {
                maxSeq = Math.max(maxSeq, Long.parseLong(tail));
            }
        }
        return prefix + String.format("%04d", maxSeq + 1);
    }

    /**
     * 当物状态联动：置成 target；requireCurrent 非 null 时只翻当前状态是它的行
     * （撤销只翻「已典当」的，别踩了别的流程置的状态）。联动行数不符即状态已被人动过，
     * 抛业务异常让整段事务回滚，票物两边都不落。
     */
    private void markCollateral(Long collateralId, CollateralStatus target, CollateralStatus requireCurrent) {
        TicketCollateralPO update = new TicketCollateralPO();
        update.setStatus(target.code());
        var wrapper = Wrappers.<TicketCollateralPO>lambdaUpdate()
                .eq(TicketCollateralPO::getId, collateralId);
        if (requireCurrent != null) {
            wrapper.eq(TicketCollateralPO::getStatus, requireCurrent.code());
        }
        int rows = ticketCollateralMapper.update(update, wrapper);
        if (rows == 0) {
            throw new BizException("当物状态已变化，本次办理未生效；请刷新后按最新状态办理");
        }
    }

    /**
     * 在全局命名锁保护下执行一段【含事务】的写入：锁由一条独立原始连接持有，
     * 在事务开始前 GET_LOCK、在事务提交/回滚之后才 RELEASE_LOCK（顺序不能颠倒）。
     *
     * 为什么锁要走独立连接而不是 MyBatis 连接：GET_LOCK 绑定连接；
     * 若用事务所在连接，Spring 提交时归还连接会立刻放锁，存在「锁已放、事务未提交」的窗口，
     * 后到的事务取号/点在当票时读不到刚提交的数据，会算出重复号、漏看在当票。
     * 独立连接持锁可把锁保到提交之后。
     */
    private <T> T inWriteLock(Supplier<T> action) {
        Connection lockConn;
        try {
            lockConn = dataSource.getConnection();
        } catch (SQLException e) {
            throw new BizException("系统繁忙，请稍后重试");
        }
        try {
            if (!namedLock(lockConn, true)) {
                throw new BizException("系统繁忙，请稍后重试");
            }
            try {
                return action.get();
            } finally {
                // 此时 action 内的事务已提交（或回滚），放锁后后到者必能看到本次写入
                namedLock(lockConn, false);
            }
        } finally {
            try {
                lockConn.close();
            } catch (SQLException ignored) {
                // 连接关闭会自动释放其上的命名锁，不影响主流程
            }
        }
    }

    /** GET_LOCK / RELEASE_LOCK；返回 MySQL 结果（1 成功）。 */
    private boolean namedLock(Connection conn, boolean get) {
        String sql = get ? "SELECT GET_LOCK(?, ?)" : "SELECT RELEASE_LOCK(?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, WRITE_LOCK);
            if (get) {
                ps.setInt(2, LOCK_WAIT_SECONDS);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int r = rs.getInt(1);
                    return !rs.wasNull() && r == 1;
                }
                return false;
            }
        } catch (SQLException e) {
            if (get) {
                throw new BizException("系统繁忙，请稍后重试");
            }
            return false;
        }
    }

    /**
     * 阻塞 DB 调用 → 响应式链路桥接器：先从 Reactor Context 取操作人，再切到 boundedElastic，
     * 操作人放进 AuditContextHolder 供审计填充（与当户/当物模块同一套约定，顺序不能颠倒）。
     */
    private <T> Mono<T> blocking(Supplier<T> supplier) {
        return Mono.deferContextual(ctx -> {
            String operator = ReactiveOperatorContext.getOperator(ctx);
            return Mono.fromCallable(() -> {
                AuditContextHolder.setOperator(operator);
                try {
                    return supplier.get();
                } finally {
                    AuditContextHolder.clear();
                }
            }).subscribeOn(Schedulers.boundedElastic());
        });
    }
}
