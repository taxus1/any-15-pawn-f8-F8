package com.somepro.infrastructure.persistence.renew;

import cn.hutool.core.util.IdUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.pagehelper.PageHelper;
import com.somepro.common.exception.BizException;
import com.somepro.domain.pawner.model.PawnerStatus;
import com.somepro.domain.renew.model.PawnRenew;
import com.somepro.domain.shared.model.PageResult;
import com.somepro.domain.renew.repository.PawnRenewRepository;
import com.somepro.domain.ticket.model.TicketStatus;
import com.somepro.infrastructure.config.ReactiveOperatorContext;
import com.somepro.infrastructure.persistence.audit.AuditContextHolder;
import com.somepro.infrastructure.persistence.renew.converter.PawnRenewPoConverter;
import com.somepro.infrastructure.persistence.renew.po.PawnRenewPO;
import com.somepro.infrastructure.persistence.ticket.po.PawnTicketPO;
import com.somepro.infrastructure.persistence.ticket.PawnTicketMapper;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 续当仓储适配器（基础设施层）：MyBatis-Plus 阻塞 JDBC 经 blocking(...) 桥接进响应式链路。
 *
 * 本类三处关键业务语义：
 *
 * 1. 同票同时点只续一条（含并发重复递交）
 *    办理先抢 MySQL 命名锁 GET_LOCK('pawn_renew:write')（全实例互斥），锁内事务里先对当票做
 *    条件更新：id 命中、状态仍是 ACTIVE、且到期日期仍是办理前那一天（old_due_date）才把
 *    到期日期推进到 new_due_date。柜台手快重复递交同一笔，第二笔读到/拿到的票面到期日期
 *    已被第一笔推进，条件不成立、更新 0 行，整段回滚 —— 续当记录只落一条，票也只推进一次。
 *    票在办理瞬间被撤销/赎回/绝当（状态离开 ACTIVE）同样挡回。
 *
 * 2. 续当单号生成 XD-yyyy-NNNN
 *    同一把写锁内：取当年续当单号的最大整数序号 +1（序号在 Java 侧解析，
 *    避免字符串排序把 9999 排在 10000 前），锁内算号天然不撞；
 *    取号刻意包含已删除的续当：单号一经分配永久占用。
 *    uk_renew_no 唯一索引是最后防线，极端瞬态冲突整段重试，不甩底层错给柜台。
 *
 * 3. 票期推进与续当登记同一事务
 *    当票到期日期推进（状态仍留在当 ACTIVE）与续当登记写入在同一事务里落库，
 *    要么一起成、要么一起回滚，不会出现「票期推了、没续当记录」或反过来的裂账。
 *
 * 4. 冻结当户挡续当
 *    与票期推进同一把写锁、同一事务内点票面当户的状态：不是正常（冻结/注销/已销户）
 *    一律不续，得先解冻再来。赎当不在此拦 —— 东西是人家的，冻结不影响赎当。
 *
 * 锁的连接与时序同当票模块：用一条【独立于事务的原始连接】在事务开启前 GET_LOCK、
 * 在事务【提交之后】才 RELEASE_LOCK，避免「锁已放、事务未提交」导致后到者漏看刚推进的到期日期。
 */
@Repository
public class PawnRenewRepositoryImpl implements PawnRenewRepository {

    /** 业务日期统一按行里所在时区算，避免容器 UTC 下单号跨年。 */
    private static final ZoneId BIZ_ZONE = ZoneId.of("Asia/Shanghai");
    /** 续当办理临界区命名锁（MySQL 全实例同名互斥）。 */
    private static final String WRITE_LOCK = "pawn_renew:write";
    private static final int LOCK_WAIT_SECONDS = 10;
    private static final int MAX_RETRY = 5;

    private final PawnRenewMapper pawnRenewMapper;
    private final PawnTicketMapper pawnTicketMapper;
    private final RenewPawnerQueryMapper renewPawnerQueryMapper;
    private final TransactionTemplate transactionTemplate;
    private final DataSource dataSource;

    public PawnRenewRepositoryImpl(PawnRenewMapper pawnRenewMapper,
                                   PawnTicketMapper pawnTicketMapper,
                                   RenewPawnerQueryMapper renewPawnerQueryMapper,
                                   PlatformTransactionManager transactionManager,
                                   DataSource dataSource) {
        this.pawnRenewMapper = pawnRenewMapper;
        this.pawnTicketMapper = pawnTicketMapper;
        this.renewPawnerQueryMapper = renewPawnerQueryMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.dataSource = dataSource;
    }

    @Override
    public Mono<PawnRenew> insert(PawnRenew renew) {
        return blocking(() -> {
            // 每轮重试用独立连接重新抢锁；兜住单号撞号 / 锁等待超时等瞬态冲突
            for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
                try {
                    return inWriteLock(() -> transactionTemplate.execute(status -> {
                        // 冻结当户挡续当：锁内点票面当户状态，不是正常一律不续（赎当照常，不走这里）
                        requireNormalPawner(renew.getTicketId());
                        // 同票同时点只续一条：条件更新「在当 + 到期日期仍是办理前那一天」才推进。
                        // 重复递交的第二笔到期日期已对不上 oldDueDate，更新 0 行，整段回滚不落记录。
                        PawnTicketPO ticketUpdate = new PawnTicketPO();
                        ticketUpdate.setDueDate(renew.getNewDueDate());
                        int rows = pawnTicketMapper.update(ticketUpdate,
                                Wrappers.<PawnTicketPO>lambdaUpdate()
                                        .eq(PawnTicketPO::getId, renew.getTicketId())
                                        .eq(PawnTicketPO::getStatus, TicketStatus.ACTIVE.code())
                                        .eq(PawnTicketPO::getDueDate, renew.getOldDueDate()));
                        if (rows == 0) {
                            throw new BizException("当票状态或到期日期已变化，本次续当未生效；请刷新后按最新票面办理");
                        }
                        PawnRenewPO po = PawnRenewPoConverter.toPo(renew);
                        po.setId(IdUtil.getSnowflakeNextId());
                        po.setRenewNo(nextRenewNo());
                        pawnRenewMapper.insert(po);
                        return PawnRenewPoConverter.toDomain(po);
                    }));
                } catch (DuplicateKeyException | TransientDataAccessException e) {
                    // uk_renew_no 是最后防线，锁内正常不会撞；撞了整段重新取号重试
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
    public Mono<PawnRenew> findById(Long id) {
        return blocking(() -> {
            PawnRenewPO po = pawnRenewMapper.selectById(id);
            return po == null ? null : PawnRenewPoConverter.toDomain(po);
        });
    }

    @Override
    public Mono<PawnRenew> findByRenewNo(String renewNo) {
        return blocking(() -> {
            PawnRenewPO po = pawnRenewMapper.selectOne(
                    Wrappers.<PawnRenewPO>lambdaQuery().eq(PawnRenewPO::getRenewNo, renewNo));
            return po == null ? null : PawnRenewPoConverter.toDomain(po);
        });
    }

    @Override
    public Mono<PageResult<PawnRenew>> pageByTicket(int pageNum, int pageSize, Long ticketId) {
        return this.<PageResult<PawnRenew>>blocking(() -> {
            try {
                PageHelper.startPage(pageNum, pageSize);
                LambdaQueryWrapper<PawnRenewPO> wrapper = Wrappers.<PawnRenewPO>lambdaQuery()
                        .eq(PawnRenewPO::getTicketId, ticketId)
                        // 稳定排序：一页页往后翻不会重复、不会跳条，续当先后也对得上办理次序
                        .orderByAsc(PawnRenewPO::getId);
                List<PawnRenewPO> rows = pawnRenewMapper.selectList(wrapper);
                long total = rows instanceof com.github.pagehelper.Page
                        ? ((com.github.pagehelper.Page<?>) rows).getTotal()
                        : rows.size();
                List<PawnRenew> content = rows.stream()
                        .map(PawnRenewPoConverter::toDomain)
                        .collect(Collectors.toList());
                return new PageResult<>(content, total, pageNum, pageSize);
            } finally {
                // PageHelper 靠 ThreadLocal 传分页参数，必须清，避免污染线程池下一次调用
                PageHelper.clearPage();
            }
        });
    }

    /**
     * 冻结/注销/已销户的当户名下当票不能续当：只在写锁事务内调用，与票期推进同一临界区。
     * 状态口径与当户档案一致（NORMAL/FROZEN/CLOSED），不另造值。
     */
    private void requireNormalPawner(Long ticketId) {
        String pawnerStatus = renewPawnerQueryMapper.findPawnerStatusByTicketId(ticketId);
        if (PawnerStatus.NORMAL.code().equals(pawnerStatus)) {
            return;
        }
        if (PawnerStatus.FROZEN.code().equals(pawnerStatus)) {
            throw new BizException("该当户已被冻结，其名下当票不能办理续当；请先解冻再办理");
        }
        if (PawnerStatus.CLOSED.code().equals(pawnerStatus)) {
            throw new BizException("该当户已注销，其名下当票不能办理续当");
        }
        throw new BizException("票面当户不存在或已销户，不能办理续当");
    }

    /**
     * 生成 XD-年份-序号：序号是当年已有续当单号（含已删除）最大整数 +1，至少 4 位、超出自然进位。
     * 只在写锁（{@link #inWriteLock}）内调用，锁内串行所以不会撞号；
     * renew_no 唯一索引是最后防线，极端瞬态冲突由外层整段重试兜底。
     */
    private String nextRenewNo() {
        int year = LocalDate.now(BIZ_ZONE).getYear();
        String prefix = "XD-" + year + "-";
        long maxSeq = 0L;
        for (String no : pawnRenewMapper.findRenewNosByPrefix(prefix + "%")) {
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
     * 在全局命名锁保护下执行一段【含事务】的写入：锁由一条独立原始连接持有，
     * 在事务开始前 GET_LOCK、在事务提交/回滚之后才 RELEASE_LOCK（顺序不能颠倒）。
     *
     * 为什么锁要走独立连接而不是 MyBatis 连接：GET_LOCK 绑定连接；
     * 若用事务所在连接，Spring 提交时归还连接会立刻放锁，存在「锁已放、事务未提交」的窗口，
     * 后到的事务取号/点到期日期时读不到刚提交的数据，会算出重复号、漏看刚推进的到期日期。
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
     * 操作人放进 AuditContextHolder 供审计填充（与当户/当票模块同一套约定，顺序不能颠倒）。
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
