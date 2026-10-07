package com.somepro.infrastructure.persistence.ticket;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 当户表只读 Mapper：当票模块只在开票落库前点一次当户状态，不做任何写入。
 * 状态口径见 doc/schema/pawn.sql：NORMAL 正常 / FROZEN 冻结 / CLOSED 注销。
 * 已删除（del_flag=1）的当户查不到，按「不存在或已销户」挡回。
 */
@Mapper
public interface TicketPawnerQueryMapper {

    /** 取当户当前状态（NORMAL/FROZEN/CLOSED）；查不到返回 null。 */
    @Select("SELECT status FROM t_pawner WHERE id = #{pawnerId} AND del_flag = 0")
    String findStatusByPawnerId(@Param("pawnerId") Long pawnerId);
}
