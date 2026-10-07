package com.somepro.domain.pawner.model;

import com.somepro.common.exception.BizException;
import com.somepro.domain.shared.model.BaseEntity;
import lombok.Getter;
import lombok.Setter;

import java.util.regex.Pattern;

/**
 * 当户聚合根（纯领域对象，不带任何持久化注解）。
 *
 * 核心不变量：
 * 1. 身份证号格式必须合法（18 位，末位可为 X）；
 * 2. 「同一张身份证在未注销档案里只准留一份」是跨聚合的唯一性，由仓储在落库时保证
 *    （见 PawnerRepositoryImpl 里的命名锁 + 锁定读）；本聚合只负责单档案自身的规则；
 * 3. 已注销（CLOSED）的档案是历史档案，不允许再改任何信息，也不能重复注销。
 *
 * 编号 pawnerNo（DH-2026-0001 样式）由仓储按当年序号生成，全局唯一。
 */
@Getter
@Setter
public class Pawner extends BaseEntity {

    /** 18 位身份证：前 17 位数字，末位数字或 X/x。不做校验码验算，避免把历史/测试数据挡在外面。 */
    private static final Pattern ID_CARD_PATTERN = Pattern.compile("^\\d{17}[\\dXx]$");

    private Long id;

    /** 当户编号，如 DH-2026-0001；注册时由仓储生成，业务上不可改。 */
    private String pawnerNo;

    private String name;

    private String idCard;

    private String phone;

    private String address;

    private PawnerStatus status;

    /**
     * 工厂方法：新录当户。默认放 NORMAL，状态不接受外部指定。
     * 身份证唯一性校验不在这里（跨聚合查库），由应用层 + 仓储负责。
     */
    public static Pawner register(String name, String idCard, String phone, String address) {
        Pawner pawner = new Pawner();
        pawner.applyProfile(name, idCard, phone, address);
        pawner.status = PawnerStatus.NORMAL;
        return pawner;
    }

    /**
     * 修改档案信息（姓名/身份证/电话/地址）。已注销档案不允许修改。
     * 身份证若变更，唯一性仍由仓储保证（与新建同一套加锁逻辑）。
     */
    public void modify(String name, String idCard, String phone, String address) {
        if (status == PawnerStatus.CLOSED) {
            throw new BizException("当户已注销，不能再修改档案");
        }
        applyProfile(name, idCard, phone, address);
    }

    /**
     * 冻结：正常 → 冻结。连着点两回是幂等空操作（已在冻结不再变），
     * 已注销的档案是历史档案，不能冻结。
     * 并发下「冻结与解冻同时点」的最终落库值由仓储的条件更新兜底，这里只守单档案规则。
     */
    public void freeze() {
        if (status == PawnerStatus.CLOSED) {
            throw new BizException("当户已注销，不能冻结");
        }
        this.status = PawnerStatus.FROZEN;
    }

    /**
     * 解冻：冻结 → 正常。连着点两回是幂等空操作（已是正常不再变）；
     * 已注销的档案不能解冻 —— 注销是终态，不在冻结/解冻这条线上，得按历史档案单独处理。
     */
    public void unfreeze() {
        if (status == PawnerStatus.CLOSED) {
            throw new BizException("当户已注销，不能解冻；注销档案需按历史档案单独处理");
        }
        this.status = PawnerStatus.NORMAL;
    }

    /** 在 NORMAL / FROZEN 之间切换；仅应用层在明确「冻结/解冻」语义时使用。 */
    public void changeStatus(PawnerStatus target) {
        if (target == null) {
            throw new BizException("状态不能为空");
        }
        if (this.status == PawnerStatus.CLOSED) {
            throw new BizException("当户已注销，不能再变更状态");
        }
        if (target == PawnerStatus.CLOSED) {
            throw new BizException("注销必须走专门的注销用例");
        }
        this.status = target;
    }

    /**
     * 注销。名下是否干净（无未了结当物、无在当当票）由应用层查对账端口先确认，
     * 这里只守档案自身的状态规则。
     */
    public void close() {
        if (this.status == PawnerStatus.CLOSED) {
            throw new BizException("当户已注销，不能重复注销");
        }
        this.status = PawnerStatus.CLOSED;
    }

    public boolean isClosed() {
        return status == PawnerStatus.CLOSED;
    }

    /** 公共赋值逻辑：统一做必填/格式校验与空白归一化（电话、地址留空按 null 存）。 */
    private void applyProfile(String name, String idCard, String phone, String address) {
        if (name == null || name.isBlank()) {
            throw new BizException("姓名不能为空");
        }
        if (idCard == null || idCard.isBlank()) {
            throw new BizException("身份证号不能为空");
        }
        String card = idCard.trim();
        if (!ID_CARD_PATTERN.matcher(card).matches()) {
            throw new BizException("身份证号格式不正确，应为 18 位");
        }
        this.name = name.trim();
        this.idCard = card;
        this.phone = normalize(phone);
        this.address = normalize(address);
    }

    private String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
