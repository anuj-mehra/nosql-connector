package com.anuj.nosqlconnector.exception;

public class HBaseDaoException extends Exception{

    private static final long serialVersionUID = 1L;

    private final String errorCode;

    private HBaseDaoException(final HBaseDaoExceptionBuilder builder){
        super(builder.errorMessage, builder.throwable);
        this.errorCode = builder.errorCode;
    }

    public String getErrorCode(){
        return this.errorCode;
    }

    public static class HBaseDaoExceptionBuilder {

        private String errorCode;
        private String errorMessage;
        private Throwable throwable;

        public HBaseDaoExceptionBuilder errorCode(final String errorCode){
            this.errorCode = errorCode;
            return this;
        }

        public HBaseDaoExceptionBuilder errorMessage(final String errorMessage){
            this.errorMessage = errorMessage;
            return this;
        }

        public HBaseDaoExceptionBuilder throwable(final Throwable throwable){
            this.throwable = throwable;
            return this;
        }

        public static HBaseDaoExceptionBuilder create(){
            return new HBaseDaoExceptionBuilder();
        }

        public HBaseDaoException build(){
            return new HBaseDaoException(this);
        }
    }
}
