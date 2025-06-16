//package ella.diktok_globle_live;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.web.bind.annotation.*;
//
//@RestController
//@RequestMapping("/api")
//public class DanmuController {
//
//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;
//
//    @PostMapping("/sendDanmu")
//    public String sendDanmu(@RequestBody DanmuReq req) {
//        kafkaTemplate.send("danmu-topic", req.getContent());
//        return "OK";
//    }
//}
//
//// 弹幕请求体对象
//class DanmuReq {
//    private String content;
//
//    public String getContent() {
//        return content;
//    }
//    public void setContent(String content) {
//        this.content = content;
//    }
//}
