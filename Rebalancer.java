import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Rebalancer {

    private final int REBALANCE_INTERVAL;
    private Controller controller;
    private Timer timer;
    private final Lock lock = new ReentrantLock();

    public Rebalancer(Integer timeout, Controller controller) {
        this.controller = controller;
        this.REBALANCE_INTERVAL = timeout;
        timer = new Timer();
        timer.schedule(new RebalanceTask(), 0, timeout);
    }

    public void rebalanceOperation() {
        // Stopping the timer
        if (timer != null) {
            timer.cancel();
            timer = null;
        }

        // TODO: write the rebalance operation


        // Reseting the timer
        resetTimer();
    }

    public void startRebalanceOperation() {
        rebalanceOperation();
    }

    public void resetTimer() {
        lock.lock();
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        timer = new Timer();
        timer.schedule(new RebalanceTask(), 0, REBALANCE_INTERVAL);
    }

    private class RebalanceTask extends TimerTask {
        @Override
        public void run() {
            rebalanceOperation();
        }
    }
}
