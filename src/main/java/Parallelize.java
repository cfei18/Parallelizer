import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by chrisfei on 5/2/14.
 */
public class Parallelize {

    public static void main(String[] args) throws Exception {
        try {
            new Parallelize(Integer.parseInt(args[1])).go(args[0]);
        } catch(ArrayIndexOutOfBoundsException | NumberFormatException e) {
            System.out.println("USAGE: java -jar parallelize.jar [fileName] [numThreads]");
        }
    }

    private class StreamWrapper extends Thread {
        InputStream is = null;
        String type = null;
        String message = null;

        public String getMessage() {
            return message;
        }

        StreamWrapper(InputStream is, String type) {
            this.is = is;
            this.type = type;
        }

        public void run() {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                StringBuffer buffer = new StringBuffer();
                String line = null;
                while ( (line = br.readLine()) != null) {
                    buffer.append(line);//.append("\n");
                }
                message = buffer.toString();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    private ExecutorService executorService;
    Parallelize(int numThreads) {
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }

    private class FutureTuple {
        String command;
        Future future;
        FutureTuple(String command, Future future) {
            this.command = command;
            this.future = future;
        }
    }

    void go(String fileName) throws Exception {
        List<FutureTuple> futures = new ArrayList<FutureTuple>();
        Scanner sc = new Scanner(new File(fileName));

        final Runtime rt = Runtime.getRuntime();

        while(sc.hasNextLine()) {
            final String line = sc.nextLine();
            if(line.contains("#!") || line.startsWith("#")) {
                log("Skipping " + line);
                continue;
            }

            Future<?> future =  this.executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        log("Running command " + line);
                        Process proc = rt.exec(line);

                        StreamWrapper error = new StreamWrapper(proc.getErrorStream(), "ERROR");
                        StreamWrapper output = new StreamWrapper(proc.getInputStream(), "OUTPUT");
                        int exitVal = 0;

                        error.start();
                        output.start();
                        error.join(3000);
                        output.join(3000);
                        exitVal = proc.waitFor();
                        if(output.message != null && !output.message.isEmpty()) {
                            log(output.message);
                        }

                        if(error.message != null && !error.message.isEmpty()) {
                            log(error.message);
                        }

                        log("Finished command " + line);
                    } catch(Exception e) {
                        log("FAILED command " + line);
                        e.printStackTrace();
                    }
                }
            });
            futures.add(new FutureTuple(line, future));
        }

        for(FutureTuple tuple : futures) {
            try {
                tuple.future.get(3, TimeUnit.MINUTES);
            } catch(TimeoutException e) {
                log("ERROR! repair timed out: " + tuple.command);
                tuple.future.cancel(true);
            }
        }

        log("Shutting down");
        this.executorService.shutdown();
    }

    void log(String line) {
        System.out.println("Thread " + Thread.currentThread().getId() + ": " + line);
    }
}
