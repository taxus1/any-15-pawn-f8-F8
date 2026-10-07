package com.somepro.infrastructure.persistence.pawner;

import cn.hutool.core.util.IdUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.pagehelper.PageHelper;
import com.somepro.common.exception.BizException;
import com.somepro.domain.pawner.model.Pawner;
import com.somepro.domain.pawner.model.PawnerQuery;
import com.somepro.domain.pawner.model.PawnerStatus;
import com.somepro.domain.pawner.repository.PawnerRepository;
import com.somepro.domain.shared.model.PageResult;
import com.somepro.infrastructure.config.ReactiveOperatorContext;
import com.somepro.infrastructure.persistence.audit.AuditContextHolder;
import com.somepro.infrastructure.persistence.pawner.converter.PawnerPoConverter;
import com.somepro.infrastructure.persistence.pawner.po.PawnerPO;
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
 * 当户仓储适配器（基础设施层）：MyBatis-Plus 阻塞 JDBC 经 blocking(...) 桥接进响应式链路。
 *
 * 本类两处关键业务语义：
 *
 * 1. 身份证未注销唯一（含并发）
 *    建表脚本只有 id_card 普通索引（要允许同证存在多条已注销历史档案），无法靠唯一索引兜底。
 *    所有写入（建档 / 改证）都先抢同一把 MySQL 命名锁 GET_LOCK('pawner:write')（全实例互斥），
 *    在锁内做未注销计数检查再写入 —— 「一眨眼涌进来两条」也只会落一份。
 *    用一把全局写锁而不是「按证号分锁」：还要防「建档」与「把档案改成这张证」两类操作互相漏看，
 *    且柜台录入量很低，临界区只有几毫秒，串行化没有吞吐问题。
 *
 *    锁的连接与时序（这里最容易出错）：命名锁绑连接，所以用一条【独立于事务的原始连接】
 *    在事务开启前 GET_LOCK，在事务【提交之后】才 RELEASE_LOCK。不能把锁放进事务内部、
 *    在提交前释放——否则第二个事务抢锁时前一个插入尚未提交，MVCC 快照读不到，唯一性形同虚设。
 *    检查用普通读（不加 FOR UPDATE）：id_card 是普通二级索引，锁定读的间隙锁会让不同证号
 *    并发插入互锁死锁。
 *
 * 2. 编号生成 DH-yyyy-NNNN
 *    同样在上述写锁内：取当年编号的最大整数序号 +1（序号在 Java 侧解析，
 *    避免字符串排序把 9999 排在 10000 前），锁内算号天然不撞；
 *    pawner_no 唯一索引是最后防线，极端情况下整段重试。
 *
 * 3. 冻结/解冻的条件迁移（幂等 + 并发）
 *    状态只走「from → to」条件更新（UPDATE ... WHERE id AND status = from），与建档/改证
 *    共用同一把 pawner:write 写锁，全实例串行：同一人连着点两回，第二回条件不命中、
 *    读到已是目标状态直接返回现状（幂等，状态不来回翻）；两人几乎同时分别点冻结/解冻，
 *    两笔在锁内排队，后落库的那笔看到的是前一笔已提交的状态，最终必是一个稳定值，
 *    不会一笔盖着一笔乱翻。迁移真正发生时，办理时刻与经办人由审计自动填充落账
 *    （update_time/update_by）；幂等空操作不动审计字段，首次冻结的时刻不被冲掉。
 */
@Repository
public class PawnerRepositoryImpl implements PawnerRepository {

    /** 业务日期统一按行里所在时区算，避免容器 UTC 下编号跨年。 */
    private static final ZoneId BIZ_ZONE = ZoneId.of("Asia/Shanghai");
    /** 当户写入临界区命名锁（建档 / 改证共用，MySQL 全实例同名互斥）。 */
    private static final String WRITE_LOCK = "pawner:write";
    private static final int LOCK_WAIT_SECONDS = 10;
    private static final int MAX_RETRY = 5;

    private final PawnerMapper pawnerMapper;
    private final TransactionTemplate transactionTemplate;
    private final DataSource dataSource;

    public PawnerRepositoryImpl(PawnerMapper pawnerMapper,
                                PlatformTransactionManager transactionManager,
                                DataSource dataSource) {
        this.pawnerMapper = pawnerMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.dataSource = dataSource;
    }

    @Override
    public Mono<Pawner> insert(Pawner pawner) {
        return blocking(() -> {
            // 每轮重试用独立连接重新抢锁；兜住编号撞号 / 锁等待超时等瞬态冲突
            for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
                try {
                    return inWriteLock(() -> transactionTemplate.execute(status -> {
                        long exists = pawnerMapper.countActiveByIdCard(pawner.getIdCard());
                        if (exists > 0) {
                            throw new BizException("该身份证已存在未注销的当户档案，不能重复建档");
                        }
                        PawnerPO po = PawnerPoConverter.toPo(pawner);
                        po.setId(IdUtil.getSnowflakeNextId());
                        po.setPawnerNo(nextPawnerNo());
                        pawnerMapper.insert(po);
                        return PawnerPoConverter.toDomain(po);
                    }));
                } catch (DuplicateKeyException | TransientDataAccessException e) {
                    // 锁内正常不会撞 uk_pawner_no；这里只兜极端瞬态冲突。身份证重复是 BizException，直接外抛
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
    public Mono<Pawner> update(Pawner pawner) {
        return blocking(() -> inWriteLock(() -> transactionTemplate.execute(status -> {
            PawnerPO existing = pawnerMapper.selectById(pawner.getId());
            if (existing == null) {
                throw new BizException("当户不存在");
            }
            long conflict = pawnerMapper.countActiveByIdCardExclude(pawner.getIdCard(), pawner.getId());
            if (conflict > 0) {
                throw new BizException("该身份证已存在其他未注销的当户档案，身份证不能重复");
            }
            // 编号永不改；审计字段沿用框架自动填充，把原值带上避免被覆盖丢
            pawner.setPawnerNo(existing.getPawnerNo());
            PawnerPO po = PawnerPoConverter.toPo(pawner);
            int rows = pawnerMapper.updateById(po);
            if (rows == 0) {
                throw new BizException("当户不存在");
            }
            PawnerPO refreshed = pawnerMapper.selectById(pawner.getId());
            return PawnerPoConverter.toDomain(Objects.requireNonNullElse(refreshed, po));
        })));
    }

    @Override
    public Mono<Pawner> findById(Long id) {
        return blocking(() -> {
            PawnerPO po = pawnerMapper.selectById(id);
            return po == null ? null : PawnerPoConverter.toDomain(po);
        });
    }

    /**
     * 冻结/解冻专用落库：写锁 + 事务内做「from → to」条件更新，再读本行分类结果。
     * 状态口径只有 NORMAL/FROZEN/CLOSED 三个值，条件未命中时现态非 to 即 CLOSED：
     * 已是 to 按幂等成功返回现状；是 CLOSED 说明档案已注销，冻结/解冻都办不了。
     */
    @Override
    public Mono<Pawner> updateStatus(Long id, PawnerStatus from, PawnerStatus to) {
        return blocking(() -> inWriteLock(() -> transactionTemplate.execute(status -> {
            PawnerPO update = new PawnerPO();
            update.setStatus(to.code());
            int rows = pawnerMapper.update(update, Wrappers.<PawnerPO>lambdaUpdate()
                    .eq(PawnerPO::getId, id)
                    .eq(PawnerPO::getStatus, from.code()));
            PawnerPO current = pawnerMapper.selectById(id);
            if (current == null) {
                throw new BizException("当户不存在");
            }
            if (rows == 0 && !to.code().equals(current.getStatus())) {
                throw new BizException("当户已注销，不能办理冻结/解冻；注销档案需按历史档案单独处理");
            }
            return PawnerPoConverter.toDomain(current);
        })));
    }

    @Override
    public Mono<Pawner> findByPawnerNo(String pawnerNo) {
        return blocking(() -> {
            PawnerPO po = pawnerMapper.selectOne(
                    Wrappers.<PawnerPO>lambdaQuery().eq(PawnerPO::getPawnerNo, pawnerNo));
            return po == null ? null : PawnerPoConverter.toDomain(po);
        });
    }

    @Override
    public Mono<PageResult<Pawner>> page(int pageNum, int pageSize, PawnerQuery query) {
        return this.<PageResult<Pawner>>blocking(() -> {
            try {
                PageHelper.startPage(pageNum, pageSize);
                LambdaQueryWrapper<PawnerPO> wrapper = Wrappers.<PawnerPO>lambdaQuery();
                if (query.name() != null) {
                    wrapper.like(PawnerPO::getName, query.name());
                }
                if (query.idCard() != null) {
                    // 身份证号没有转义需求（只含数字/X），直接模糊匹配
                    wrapper.like(PawnerPO::getIdCard, query.idCard());
                }
                if (query.phone() != null) {
                    wrapper.like(PawnerPO::getPhone, query.phone());
                }
                if (query.status() != null) {
                    wrapper.eq(PawnerPO::getStatus, query.status().code());
                } else {
                    // 不传状态 = 日常名册：正常 + 冻结都看得到，已注销的不出现
                    wrapper.ne(PawnerPO::getStatus, PawnerStatus.CLOSED.code());
                }
                // 稳定排序：两页之间不会出现同一个人，分页结果可重复对号
                wrapper.orderByAsc(PawnerPO::getId);
                List<PawnerPO> rows = pawnerMapper.selectList(wrapper);
                long total = rows instanceof com.github.pagehelper.Page
                        ? ((com.github.pagehelper.Page<?>) rows).getTotal()
                        : rows.size();
                List<Pawner> content = rows.stream()
                        .map(PawnerPoConverter::toDomain)
                        .collect(Collectors.toList());
                return new PageResult<>(content, total, pageNum, pageSize);
            } finally {
                // PageHelper 靠 ThreadLocal 传分页参数，必须清，避免污染线程池下一次调用
                PageHelper.clearPage();
            }
        });
    }

    /**
     * 生成 DH-年份-序号：序号是当年已有编号最大整数 +1，至少 4 位、超出自然进位。
     * 只在写锁（{@link #inWriteLock}）内调用，锁内串行所以不会撞号；
     * pawner_no 唯一索引是最后防线，极端瞬态冲突由外层整段重试兜底。
     */
    private String nextPawnerNo() {
        int year = LocalDate.now(BIZ_ZONE).getYear();
        String prefix = "DH-" + year + "-";
        long maxSeq = 0L;
        for (String no : pawnerMapper.findPawnerNosByPrefix(prefix + "%")) {
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
     * 后到的事务 MVCC 快照读不到未提交插入，唯一性会漏。独立连接持锁可把锁保到提交之后。
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
     * 操作人放进 AuditContextHolder 供审计填充（与 demo 模块同一套约定，顺序不能颠倒）。
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
