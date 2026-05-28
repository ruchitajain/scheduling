package scheduler;

import lombok.AllArgsConstructor;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@AllArgsConstructor
class ScheduledJob{
    int jobId;
    long runAtMills;
    Runnable task;
}

public class ReentrantLockScheduler {
    PriorityQueue<ScheduledJob> minHeap = new PriorityQueue<>(Comparator.comparingLong(j -> j.runAtMills));
    Map<Integer, ScheduledJob> jobMap = new HashMap<>();
    ReentrantLock lock = new ReentrantLock();
    Condition newJobCondition = lock.newCondition();
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    volatile boolean shutdown = false;
    public ReentrantLockScheduler(){
        Thread threadSchedulre = new Thread(this::schedulerLoop);
        threadSchedulre.start();
    }
    public void schedule(int jobId, long runAtMills, Runnable taks){
        ScheduledJob job = new ScheduledJob(jobId,runAtMills, taks);
        lock.lock();
        try {
            if (jobMap.containsKey(jobId)){
                throw new IllegalArgumentException("Duplicate Job");
            }
            ScheduledJob currentJob = minHeap.peek();
            minHeap.offer(job);
            jobMap.put(jobId,job);
            if (currentJob == null || currentJob.runAtMills>runAtMills){
                newJobCondition.signal();
            }
        }finally {
            lock.unlock();
        }
    }
    public void schedulerLoop(){
        while (!shutdown){
            lock.lock();
            try {
                while (minHeap.isEmpty()){
                    newJobCondition.await();
                }
                ScheduledJob nextJob = minHeap.peek();
                if ( (nextJob.runAtMills - System.currentTimeMillis())>0 ){
                    newJobCondition.await( (nextJob.runAtMills - System.currentTimeMillis()) , TimeUnit.MILLISECONDS);
                    continue;
                }
                minHeap.poll();
                jobMap.remove(nextJob.jobId);
                executorService.submit(nextJob.task);
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
            } finally{
                lock.unlock();
            }

        }
    }

    public void shutdown() {
        shutdown=true;
        lock.lock();
        try {
            newJobCondition.signalAll();
        }finally {
            lock.unlock();
        }
        executorService.shutdown();
    }

    public static void main(String[] args)
            throws Exception {

        ReentrantLockScheduler scheduler =
                new ReentrantLockScheduler();

        scheduler.schedule(
                1,
                System.currentTimeMillis() + 5000,
                () -> System.out.println(
                        "Task 1 " + System.currentTimeMillis()
                )
        );

        scheduler.schedule(
                2,
                System.currentTimeMillis() + 2000,
                () -> System.out.println(
                        "Task 2 " + System.currentTimeMillis()
                )
        );

        scheduler.schedule(
                3,
                System.currentTimeMillis() + 8000,
                () -> System.out.println(
                        "Task 3 " + System.currentTimeMillis()
                )
        );
    }
}
