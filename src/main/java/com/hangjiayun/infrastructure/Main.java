package com.hangjiayun.infrastructure;

import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static AtomicInteger num = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        Runnable runnable = () -> {
            for (int i = 0;i<200000000;i++) {
                num.getAndAdd(1);
            }
        };
        Thread t1 = new Thread(runnable, "Thread1");
        Thread t2 = new Thread(runnable, "Thread2");
        t1.start();
        t2.start();
        System.out.println("主线程开始睡觉....");
        Thread.sleep(1000);
        System.out.println("主线程睡醒了....");
        System.out.println("主线程打印num " + num);
    }
}