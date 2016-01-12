package com.bc.calvalus.wpsrest.exception;

/**
 * @author hans
 */
public class ProductSetsNotAvailableException extends Exception {

    public ProductSetsNotAvailableException(String message) {
        super(message);
    }

    public ProductSetsNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProductSetsNotAvailableException(Throwable cause) {
        super(cause);
    }

}
