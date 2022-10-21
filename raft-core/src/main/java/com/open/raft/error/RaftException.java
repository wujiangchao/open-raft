
package com.open.raft.error;

import com.open.raft.Status;
import com.open.raft.entity.EnumOutter;

/**
 * A raft exception.
 *
 */
public class RaftException extends Throwable {

    private static final long    serialVersionUID = -1533343555230409704L;

    /**
     * Error type
     */
    private EnumOutter.ErrorType type;
    /** Error status*/
    private Status               status           = Status.OK();

    public RaftException(EnumOutter.ErrorType type) {
        super(type.name());
        this.type = type;
    }

    public RaftException(EnumOutter.ErrorType type, Status status) {
        super(status != null ? status.getErrorMsg() : type.name());
        this.type = type;
        this.status = status;
    }

    public RaftException(EnumOutter.ErrorType type, RaftError err, String fmt, Object... args) {
        super(String.format(fmt, args));
        this.type = type;
        this.status = new Status(err, fmt, args);
    }

    public RaftException() {
        this.type = EnumOutter.ErrorType.ERROR_TYPE_NONE;
        this.status = Status.OK();
    }

    public EnumOutter.ErrorType getType() {
        return this.type;
    }

    public void setType(EnumOutter.ErrorType type) {
        this.type = type;
    }

    public Status getStatus() {
        return this.status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Error [type=" + this.type + ", status=" + this.status + "]";
    }
}
