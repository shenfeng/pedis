import com.google.gson.Gson;
import com.listdb.Listdb;
import com.listdb.PushArg;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by feng on 3/24/17.
 */
public class ImportFromFile {
    public class LogItem {
        // action
        public String t;

        public String p;
        public String p1;
        public String p2;
        public String p3;
        public String p12;

        // 来源
        public String g;
        public long ts;

        // appid
        public long a;
    }


    private static final Logger logger = LoggerFactory.getLogger(ImportFromFile.class);

    @Option(name = "-h", usage = "Print help and exits")
    protected boolean help = false;

    @Option(name = "-skip", usage = "Print help and exits")
    protected int skip = 6;

    @Option(name = "-t", usage = "Threads")
    private int threads = 4;

    @Option(name = "-c", usage = "server")
    private String cserver = "";

    @Option(name = "-b", usage = "server")
    private String bserver = "";

    private static final BlockingQueue<PushArg> forBoss = new ArrayBlockingQueue<>(10240);
    private static final BlockingQueue<PushArg> forGeek = new ArrayBlockingQueue<>(10240);
    private AtomicBoolean finished = new AtomicBoolean(false);

    static class Item {
        private static final Gson gson = new Gson();
        public final int identity;
        public final String uid;
        public final String action;
        public final String val;
        private final LogItem data;

        public Item(String line) {
            String[] parts = line.split("\t");

            // identity, uid, action, val = line.split('\t')
            this.identity = Integer.parseInt(parts[0]);
            this.uid = parts[1];
            this.action = parts[2];
            this.val = parts[3];

            this.data = gson.fromJson(parts[3], LogItem.class);
        }

        public int getDb() {
            if (data.t.startsWith("_")) return 1;
            if (action.contains("chat")) return 2;
            if (action.equals("list-boss") || action.equals("list-geek") || action.equals("list-notify")) return 3;
            return 0;
        }

        public void queue() throws InterruptedException {
            PushArg arg = new PushArg(uid, Arrays.asList(val), getDb());
            if (this.identity == 9) {
                forGeek.put(arg);
            } else {
                forBoss.put(arg);
            }
        }
    }

    public Listdb.Client getClient(String ip) throws TTransportException {
        String[] parts = ip.split(":");
        TSocket trans = new TSocket(parts[0], Integer.parseInt(parts[1]), 3000);


        trans.open();
        TBinaryProtocol protocol = new TBinaryProtocol(new TFramedTransport(trans));
        return new Listdb.Client(protocol);
    }

    public void run() throws InterruptedException, TTransportException, IOException {

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
                return o1.getName().compareTo(o2.getName());
//                return Long.compare(o1.lastModified(), o2.lastModified());
            }
        });

        if (allFiles.size() > this.skip) {
            allFiles = allFiles.subList(0, allFiles.size() - this.skip);
        } else {
            return;
        }

        logger.info("finding {} files, {} threads", allFiles.size(), this.threads);
        final ExecutorService pool = Executors.newFixedThreadPool(6);

        for (int i = 0; i < 3; i++) {
            final Listdb.Client b = getClient(this.bserver);
            final Listdb.Client c = getClient(this.cserver);
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    commit2db(c, forGeek);
                }
            });
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    commit2db(b, forBoss);
                }
            });
        }

        int total = 0;
        for (File f : allFiles) {
            logger.info("doing {}", f.getName());
            BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
            String line;
            int n = 0;
            long start = System.currentTimeMillis();

            while ((line = bufferedReader.readLine()) != null) {
                n += 1;
                total += 1;
                new Item(line).queue();
            }

            f.renameTo(new File("backup/" + f.getName()));
            logger.info("done {}, total {} lines, this file {}, {} qps",
                    f.getName(), total, n, n * 1000.0 / (System.currentTimeMillis() - start));

        }
        finished.set(true);

        pool.shutdown();
        pool.awaitTermination(2, TimeUnit.DAYS);
    }

    private void commit2db(Listdb.Client client, BlockingQueue<PushArg> queue) {
        try {
            while (!queue.isEmpty() || !finished.get()) {
                int size = queue.size();
                List<PushArg> batch = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    batch.add(queue.take());
                }
                client.Pushs(batch);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
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
