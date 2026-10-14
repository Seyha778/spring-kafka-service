package com.apextech.springkafka.dto.response;

public class ResponseMessageBuilder<T> {

    private boolean success;
    private String message;
    private T data;

    public ResponseMessageBuilder<T> success() {
        this.success = true;
        this.message = "Success";
        return this;
    }

    public ResponseMessageBuilder<T> message(String message) {
        this.message = message;
        return this;
    }

    public ResponseMessageBuilder<T> data(T data) {
        this.data = data;
        return this;
    }

    public ResponseMessage<T> build() {
        return ResponseMessage.<T>builder()
                .success(success)
                .message(message)
                .data(data)
                .build();
    }
}
