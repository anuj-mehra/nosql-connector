package com.anuj.nosqlconnector.exception;

public class HBaseDataIntegrationException extends Exception{

    private static final long serialVersionUID = 1L;

    private final String errorCode;

    private HBaseDataIntegrationException(final HBaseDataIntegrationExceptionBuilder builder){
        super(builder.errorMessage, builder.throwable);
        this.errorCode = builder.errorCode;
    }

    public String getErrorCode(){
        return this.errorCode;
    }

    public static class HBaseDataIntegrationExceptionBuilder {

        private String errorCode;
        private String errorMessage;
        private Throwable throwable;

        public HBaseDataIntegrationExceptionBuilder errorCode(final String errorCode){
            this.errorCode = errorCode;
            return this;
        }

        public HBaseDataIntegrationExceptionBuilder errorMessage(final String errorMessage){
            this.errorMessage = errorMessage;
            return this;
        }

        public HBaseDataIntegrationExceptionBuilder throwable(final Throwable throwable){
            this.throwable = throwable;
            return this;
        }

        public static HBaseDataIntegrationExceptionBuilder create(){
            return new HBaseDataIntegrationExceptionBuilder();
        }

        public HBaseDataIntegrationException build(){
            return new HBaseDataIntegrationException(this);
        }
    }


}
