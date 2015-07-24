package com.xunge.persistence.hbase;

import java.util.concurrent.ThreadFactory;

/**
 * 守护线程
 *
 * @author stereo
 */
public class Daemon extends Thread {
    {
        setDaemon(true);
    }

    public static class DaemonFactory extends Daemon implements ThreadFactory {

        @Override
        public Thread newThread(Runnable runnable) {
            return new Daemon(runnable);
        }
    }

    Runnable runnable = null;

    public Daemon() {
        super();
    }

    public Daemon(Runnable runnable) {
        super(runnable);
        this.runnable = runnable;
        this.setName(((Object) runnable).toString());
    }

    public Daemon(ThreadGroup group, Runnable runnable) {
        super(group, runnable);
        this.runnable = runnable;
        this.setName(((Object) runnable).toString());
    }

    public Runnable getRunnable() {
        return runnable;
    }
}
