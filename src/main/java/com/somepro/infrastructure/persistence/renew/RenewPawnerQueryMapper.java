package com.somepro.infrastructure.persistence.renew;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 当户表只读 Mapper：续当模块只在办理落库前点一次票面当户的状态，不做任何写入。
 * 状态口径见 doc/schema/pawn.sql：NORMAL 正常 / FROZEN 冻结 / CLOSED 注销。
 * 票面当户已删除（del_flag=1）时查不到，按「不存在或已销户」挡回。
 */
@Mapper
public interface RenewPawnerQueryMapper {

    /** 取当票所属当户的当前状态（NORMAL/FROZEN/CLOSED）；查不到返回 null。 */
    @Select("SELECT p.status FROM t_pawn_ticket t JOIN t_pawner p ON p.id = t.pawner_id "
            + "WHERE t.id = #{ticketId} AND t.del_flag = 0 AND p.del_flag = 0")
    String findPawnerStatusByTicketId(@Param("ticketId") Long ticketId);
}
