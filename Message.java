import java.net.Socket;

class Message {
    private final String message;
    private final Socket reuqester;

    public Message(String message, Socket requester) {
        this.message = message;
        this.reuqester = requester;
    }

    public String getMessage() {
        return message;
    }

    public Socket getReuqester() {
        return reuqester;
    }
}