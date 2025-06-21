package core.bean;

/**
 * 接口请求返回数据
 * Created by xuwei
 */
public class Status {
    /**
     * 返回状态码
     * 200是成功，其它是异常
     */
    private int status = 200;
    /**
     * 具体的错误信息
     */
    private String msg = "success";

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
