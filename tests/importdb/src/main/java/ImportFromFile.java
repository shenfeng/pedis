import com.google.gson.Gson;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by feng on 3/24/17.
 */
public class ImportFromFile {

    private static final Logger logger = LoggerFactory.getLogger(ImportFromFile.class);

    @Option(name = "-h", usage = "Print help and exits")
    protected boolean help = false;

    @Option(name = "-t", usage = "Threads")
    private int threads = 4;

    @Option(name = "-c", usage = "server")
    private String cserver = "";

    @Option(name = "-b", usage = "server")
    private String bserver = "";

    static class Item {
        private static final Gson gson = new Gson();

        public final int identity;
        public final String uid;
        public final String action;
        public final String val;

        public Item(String line) {
            String[] parts = line.split("\t");

            // identity, uid, action, val = line.split('\t')
            this.identity = Integer.parseInt(parts[0]);
            this.uid = parts[1];
            this.action = parts[2];
            this.val = gson.fromJson(parts[3], String.class);
        }

        public int getDb() {
            if (val.startsWith("_")) return 3;
            if (action.contains("chat")) return 1;
            if (action.equals("list-boss") || action.equals("list-geek") || action.equals("list-notify")) return 2;
            return 0;
        }

        public void send(Listdb.Client bclient, Listdb.Client cclient) throws TException {
            PushArg arg = new PushArg(uid, Arrays.asList(val));
            arg.setDb(getDb());
            if (this.identity == 9) {
                cclient.Push(arg);
            } else {
                bclient.Push(arg);
            }
        }
    }

    private ConcurrentLinkedQueue<File> queue;

    public Listdb.Client getClient(String ip) throws TTransportException {
        String[] parts = ip.split(":");
        TSocket trans = new TSocket(parts[0], Integer.parseInt(parts[1]), 3000);


        trans.open();
        TBinaryProtocol protocol = new TBinaryProtocol(new TFramedTransport(trans));
        Listdb.Client client = new Listdb.Client(protocol);
        return client;
    }

    public void run() throws InterruptedException {
        File[] files = new File(".").listFiles();
        if (files == null) return;

        List<File> allFiles = new ArrayList<>();
        for (File f : files) {
            if (f.getName().startsWith("dump_to_file_bg")) {
                allFiles.add(f);
            }
        }

        Collections.sort(allFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return Long.compare(o1.lastModified(), o2.lastModified());
            }
        });

        if (allFiles.size() > 5) {
            allFiles = allFiles.subList(0, allFiles.size() - 6);
        } else {
            return;
        }

        queue = new ConcurrentLinkedQueue<>(allFiles);
        logger.info("finding {} files, {} threads", queue.size(), this.threads);
        final ExecutorService pool = Executors.newFixedThreadPool(this.threads);

        for (int i = 0; i < this.threads; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        loadAndAddToDb();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(2, TimeUnit.DAYS);
    }

    private void loadAndAddToDb() throws TException, IOException {
        Listdb.Client b = getClient(this.bserver);
        Listdb.Client c = getClient(this.cserver);
        int total = 0;

        while (true) {
            int n = 0;
            long start = System.currentTimeMillis();

            File f = queue.poll();
            if (f == null) {
                return;
            }

            logger.info("doing {}", f.getName());
            BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                n += 1;
                total += 1;
                new Item(line).send(b, c);
            }

            f.renameTo(new File("backup/" + f.getName()));

            logger.info("done {}, total {} lines, this file {}, {} qps",
                    f.getName(), total, n, n * 1000.0 / (System.currentTimeMillis() - start));
        }
    }

    public void parseArgsAndRun(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(120);

        try {
            parser.parseArgument(args);
            if (this.help) {
                System.err.println("java " + this.getClass().getCanonicalName() + " [options...] arguments...");
                parser.printUsage(System.err);
                System.exit(1);
            } else {
                this.run();
            }
        } catch (Exception e) {
        }
    }


    public static void main(String[] args) {
        new ImportFromFile().parseArgsAndRun(args);
    }
}
