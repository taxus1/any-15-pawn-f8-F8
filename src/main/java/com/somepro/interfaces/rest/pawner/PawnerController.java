package com.somepro.interfaces.rest.pawner;

import com.somepro.application.pawner.PawnerAppService;
import com.somepro.common.Result;
import com.somepro.interfaces.rest.common.vo.PageVO;
import com.somepro.interfaces.rest.pawner.dto.PawnerCreateRequest;
import com.somepro.interfaces.rest.pawner.dto.PawnerIdRequest;
import com.somepro.interfaces.rest.pawner.dto.PawnerUpdateRequest;
import com.somepro.interfaces.rest.pawner.converter.PawnerVoConverter;
import com.somepro.interfaces.rest.pawner.vo.PawnerDetailVO;
import com.somepro.interfaces.rest.pawner.vo.PawnerVO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * 当户模块用户接口层：录入、修改、详情、注销、按条件翻名单。
 *
 * 只做协议适配（参数解析、VO 转换、Result 包装），业务编排在 {@link PawnerAppService}。
 * 入参统一用 @RequestParam：表单 / query string / x-www-form-urlencoded 都能接，便于柜台端直接调用。
 */
@RestController
@RequestMapping("/api/pawner")
public class PawnerController {

    private final PawnerAppService pawnerAppService;

    public PawnerController(PawnerAppService pawnerAppService) {
        this.pawnerAppService = pawnerAppService;
    }

    /** 录入当户：状态固定默认 NORMAL，编号由服务端按 DH-年份-序号 生成。 */
    @PostMapping("/create")
    public Mono<Result<PawnerVO>> create(@ModelAttribute PawnerCreateRequest request) {
        return pawnerAppService.register(request.getName(), request.getIdCard(),
                        request.getPhone(), request.getAddress())
                .map(PawnerVoConverter::toVo)
                .map(Result::ok);
    }

    /** 修改档案：字段都传全量；status 只用于在 NORMAL/FROZEN 间冻结解冻，注销走 /close，不传则不改状态。 */
    @PostMapping("/update")
    public Mono<Result<PawnerVO>> update(@ModelAttribute PawnerUpdateRequest request) {
        return pawnerAppService.update(request.getId(), request.getName(), request.getIdCard(),
                        request.getPhone(), request.getAddress(), request.getStatus())
                .map(PawnerVoConverter::toVo)
                .map(Result::ok);
    }

    /** 注销：名下还有未了结当物或在当当票时由应用层挡回。 */
    @PostMapping("/close")
    public Mono<Result<PawnerVO>> close(@ModelAttribute PawnerIdRequest request) {
        return pawnerAppService.close(request.getId())
                .map(PawnerVoConverter::toVo)
                .map(Result::ok);
    }

    /**
     * 冻结：正常 → 冻结，重复点状态不再变；办理时刻与经办人由审计落账。
     * 冻结后名下开不了新当票、办不了续当，赎当照常。
     */
    @PostMapping("/freeze")
    public Mono<Result<PawnerVO>> freeze(@ModelAttribute PawnerIdRequest request) {
        return pawnerAppService.freeze(request.getId())
                .map(PawnerVoConverter::toVo)
                .map(Result::ok);
    }

    /** 解冻：冻结 → 正常，重复点状态不再变；已注销的当户不能解，会单独提示。 */
    @PostMapping("/unfreeze")
    public Mono<Result<PawnerVO>> unfreeze(@ModelAttribute PawnerIdRequest request) {
        return pawnerAppService.unfreeze(request.getId())
                .map(PawnerVoConverter::toVo)
                .map(Result::ok);
    }

    /**
     * 翻冻结名单：默认只出冻结中的当户，也可显式按状态查（NORMAL/FROZEN）；
     * 已注销、已删除的当户不进这份名单。一页一页走，每行带 pawnerNo 便于柜面盯人。
     */
    @GetMapping("/freeze/list")
    public Mono<Result<PageVO<PawnerVO>>> freezeList(@RequestParam(defaultValue = "1") int pageNum,
                                                     @RequestParam(defaultValue = "20") int pageSize,
                                                     @RequestParam(required = false) String status) {
        return pawnerAppService.freezePage(pageNum, pageSize, status)
                .map(PawnerVoConverter::toPageVo)
                .map(Result::ok);
    }

    /** 详情：id 或 pawnerNo 任一指定；带出在押/在库/在当当票三个对账数。 */
    @GetMapping("/detail")
    public Mono<Result<PawnerDetailVO>> detail(@RequestParam(required = false) Long id,
                                               @RequestParam(required = false) String pawnerNo) {
        return pawnerAppService.detail(id, pawnerNo)
                .map(PawnerVoConverter::toDetailVo)
                .map(Result::ok);
    }

    /**
     * 翻名单：姓名/身份证/电话/状态随意拼，都不填翻整份（默认不含已注销，显式 status=CLOSED 可调出）。
     * pageNum/pageSize 由请求说了算，每行带 pawnerNo 便于与纸质登记本对号。
     */
    @GetMapping("/list")
    public Mono<Result<PageVO<PawnerVO>>> list(@RequestParam(defaultValue = "1") int pageNum,
                                               @RequestParam(defaultValue = "20") int pageSize,
                                               @RequestParam(required = false) String name,
                                               @RequestParam(required = false) String idCard,
                                               @RequestParam(required = false) String phone,
                                               @RequestParam(required = false) String status) {
        return pawnerAppService.page(pageNum, pageSize, name, idCard, phone, status)
                .map(PawnerVoConverter::toPageVo)
                .map(Result::ok);
    }
}
