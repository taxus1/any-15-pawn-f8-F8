package com.somepro.application.pawner;

import com.somepro.common.exception.BizException;
import com.somepro.domain.pawner.model.Pawner;
import com.somepro.domain.pawner.model.PawnerLedger;
import com.somepro.domain.pawner.model.PawnerQuery;
import com.somepro.domain.pawner.model.PawnerStatus;
import com.somepro.domain.pawner.repository.PawnerLedgerPort;
import com.somepro.domain.pawner.repository.PawnerRepository;
import com.somepro.domain.shared.model.PageResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * 当户应用服务：编排录入、修改、详情、注销、冻结、解冻、翻名单等用例，不写表映射。
 *
 * 出入参用领域对象/基础类型，不认识 PO 与 VO。
 * 唯一性、编号生成等落库规则在仓储里；档案自身规则在 Pawner 聚合里；
 * 注销前「名下是否干净」在这里编排：先查对账端口，再调聚合的注销行为。
 * 冻结/解冻在这里收口：聚合守「已注销不能动」，仓储用条件迁移兜幂等与并发。
 */
@Service
public class PawnerAppService {

    private final PawnerRepository pawnerRepository;
    private final PawnerLedgerPort ledgerPort;

    public PawnerAppService(PawnerRepository pawnerRepository, PawnerLedgerPort ledgerPort) {
        this.pawnerRepository = pawnerRepository;
        this.ledgerPort = ledgerPort;
    }

    /** 录入：默认 NORMAL；同身份证已有未注销档案由仓储挡回，已注销的历史档案不影响重新建档。 */
    public Mono<Pawner> register(String name, String idCard, String phone, String address) {
        Pawner pawner = Pawner.register(name, idCard, phone, address);
        return pawnerRepository.insert(pawner);
    }

    /** 修改档案：已注销的档案改不动；改身份证同样受未注销唯一约束（排除自身）。 */
    public Mono<Pawner> update(Long id, String name, String idCard, String phone, String address, String status) {
        return requirePawner(id).flatMap(pawner -> {
            pawner.modify(name, idCard, phone, address);
            if (status != null) {
                pawner.changeStatus(PawnerStatus.ofCode(status));
            }
            return pawnerRepository.update(pawner);
        });
    }

    /** 详情：带出档案 + 名下对账三个数（在押/在库/在当当票），数与当物、当票两表实时一致。 */
    public Mono<PawnerDetail> detail(Long id, String pawnerNo) {
        Mono<Pawner> found;
        if (id != null) {
            found = requirePawner(id);
        } else if (pawnerNo != null && !pawnerNo.isBlank()) {
            found = pawnerRepository.findByPawnerNo(pawnerNo.trim())
                    .switchIfEmpty(Mono.error(new BizException("当户不存在")));
        } else {
            return Mono.error(new BizException("请指定要查看的当户（id 或 pawnerNo）"));
        }
        return found.flatMap(pawner -> ledgerPort.load(pawner.getId())
                .map(ledger -> new PawnerDetail(pawner, ledger)));
    }

    /**
     * 注销：名下还压着没走完的当物（IN_STOCK/PAWNED），或还挂着在当当票（ACTIVE），一律挡回；
     * 干干净净才销得动。销完状态置 CLOSED（账留着），默认名单不再出现。
     */
    public Mono<Pawner> close(Long id) {
        return requirePawner(id).flatMap(pawner -> ledgerPort.load(pawner.getId()).flatMap(ledger -> {
            if (!ledger.clean()) {
                return Mono.error(new BizException(String.format(
                        "该当户名下尚有未了结业务（在押当物 %d 件、在当当票 %d 笔），不能注销",
                        ledger.heldItemCount(), ledger.activeTicketCount())));
            }
            pawner.close();
            return pawnerRepository.update(pawner);
        }));
    }

    /** 翻名单：姓名/身份证/电话/状态随意拼，都不填翻整份；默认不含已注销。 */
    public Mono<PageResult<Pawner>> page(int pageNum, int pageSize,
                                         String name, String idCard, String phone, String status) {
        if (pageNum < 1 || pageSize < 1) {
            return Mono.error(new BizException("页码与每页条数必须为正整数"));
        }
        PawnerQuery query = PawnerQuery.of(name, idCard, phone, PawnerStatus.ofCode(status));
        return pawnerRepository.page(pageNum, pageSize, query);
    }

    /**
     * 冻结：正常 → 冻结。连着点两回状态不再变（幂等）；两人同时点冻结/解冻，
     * 落库必为一个稳定值（仓储条件迁移兜底）。冻结后该当户开不了新当票、办不了续当，
     * 赎当照常。办理时刻与经办人由审计字段（update_time/update_by）落账。
     */
    public Mono<Pawner> freeze(Long id) {
        if (id == null) {
            return Mono.error(new BizException("必须指定要冻结的当户"));
        }
        return requirePawner(id).flatMap(pawner -> {
            pawner.freeze();
            return pawnerRepository.updateStatus(id, PawnerStatus.NORMAL, PawnerStatus.FROZEN);
        });
    }

    /**
     * 解冻：冻结 → 正常。连着点两回状态不再变（幂等）；
     * 已注销的当户不能解冻，由聚合单独说明（注销是终态，得按历史档案另行处理）。
     */
    public Mono<Pawner> unfreeze(Long id) {
        if (id == null) {
            return Mono.error(new BizException("必须指定要解冻的当户"));
        }
        return requirePawner(id).flatMap(pawner -> {
            pawner.unfreeze();
            return pawnerRepository.updateStatus(id, PawnerStatus.FROZEN, PawnerStatus.NORMAL);
        });
    }

    /**
     * 翻冻结名单：默认只翻冻结中的当户；显式按状态查也只认 NORMAL/FROZEN。
     * 已注销（CLOSED）与已删除（del_flag=1）的当户一律不进这份名单。
     */
    public Mono<PageResult<Pawner>> freezePage(int pageNum, int pageSize, String status) {
        if (pageNum < 1 || pageSize < 1) {
            return Mono.error(new BizException("页码与每页条数必须为正整数"));
        }
        PawnerStatus parsed = (status == null || status.isBlank())
                ? PawnerStatus.FROZEN
                : PawnerStatus.ofCode(status.trim());
        if (parsed == PawnerStatus.CLOSED) {
            return Mono.error(new BizException("已注销的当户不进冻结名单，请到当户名册另行查看"));
        }
        return pawnerRepository.page(pageNum, pageSize, PawnerQuery.of(null, null, null, parsed));
    }

    private Mono<Pawner> requirePawner(Long id) {
        return pawnerRepository.findById(id)
                .switchIfEmpty(Mono.error(new BizException("当户不存在")));
    }

    /** 应用层内部组合值：档案 + 对账快照，接口层据此转详情 VO。 */
    public record PawnerDetail(Pawner pawner, PawnerLedger ledger) {
    }
}
