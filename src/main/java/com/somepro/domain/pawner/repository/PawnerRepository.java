package com.somepro.domain.pawner.repository;

import com.somepro.domain.pawner.model.Pawner;
import com.somepro.domain.pawner.model.PawnerQuery;
import com.somepro.domain.pawner.model.PawnerStatus;
import com.somepro.domain.shared.model.PageResult;
import reactor.core.publisher.Mono;

/**
 * 当户聚合的仓储端口（领域层定义，基础设施层实现）。
 *
 * 唯一性约束（同一张身份证在未注销档案里只准一份）由 {@link #insert} / {@link #update} 的实现保证：
 * 写入临界区由 MySQL 命名锁在全实例串行化，锁内做未注销计数检查，保证并发热点登记只落一份。
 */
public interface PawnerRepository {

    /**
     * 新建档案：生成全局唯一编号（DH-年份-序号）并落库。
     * 若该身份证已存在未注销（NORMAL/FROZEN）档案，抛业务异常挡回；
     * 历史上有已注销档案不拦截（允许同证重新建档）。
     */
    Mono<Pawner> insert(Pawner pawner);

    /**
     * 修改档案（含身份证变更）：与新建共用「同身份证 + 命名锁」的临界区，
     * 存在性检查排除自身；命中未注销的他人档案时抛业务异常。
     */
    Mono<Pawner> update(Pawner pawner);

    Mono<Pawner> findById(Long id);

    Mono<Pawner> findByPawnerNo(String pawnerNo);

    /**
     * 状态条件迁移（冻结/解冻专用）：仅当当前状态仍是 from 时才翻到 to。
     *
     * 幂等与并发语义：同一操作连着点两回，第二回读到已是 to 直接返回现状，状态不来回翻；
     * 两人几乎同时分别点冻结/解冻，条件更新在写锁内串行落库，最终必为二者之一的稳定值，
     * 不会一笔盖着一笔乱翻。当前状态既非 from 也非 to（只可能是已注销）时抛业务异常。
     * 迁移成功时办理时刻与经办人由审计字段（update_time/update_by）自动落账。
     */
    Mono<Pawner> updateStatus(Long id, PawnerStatus from, PawnerStatus to);

    /** 按条件翻名单；条件中 status 为 null 时只出 NORMAL/FROZEN，不出 CLOSED。 */
    Mono<PageResult<Pawner>> page(int pageNum, int pageSize, PawnerQuery query);
}
