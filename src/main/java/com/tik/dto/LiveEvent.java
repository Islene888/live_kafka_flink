// 可以放在一个新的 package BackEnd.dto;
package com.tik.dto;

// 使用Lombok可以简化代码，这里为了清晰展示，手动添加getter/setter
public class LiveEvent {
    private String userId;
    private String eventType;
    private long timestamp;
    private EventData data; // 使用一个内部类来表示不同事件的数据

    // Getters and Setters ...

    public static class EventData {
        private String text; // For comments
        private String giftId; // For gifts
        private int value; // For gifts

        // Getters and Setters ...
    }
}