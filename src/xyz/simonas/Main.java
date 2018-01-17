package xyz.simonas;

import org.jcsp.lang.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static One2OneChannel resultChannel = Channel.one2one();
    public static ProcessController B;

    public static final int READ_GROUP_COUNT = 5;
    public static final int REMOVE_GROUP_COUNT = 5;

    public static int readProcessDoneCount = 0;
    public static int failedRemoveCount = 0;

    private static final String DATA_FULL = "SankauskasS_L3_1.txt";
    private static final String DATA_NO = "SankauskasS_L3_2.txt";
    private static final String DATA_SEMI = "SankauskasS_L3_3.txt";

    private static final String RESULTS = "SankauskasS_L3a_rez.txt";

    public static void main(String[] args) {
        new Main().start(DATA_FULL);
//        new Main().start(DATA_NO);
//        new Main().start(DATA_SEMI);
    }

    private void start(String filePath) {
        List<ChannelOutput> readChannelOutput = new ArrayList<>();
        List<ChannelOutput> removeChannelOutput = new ArrayList<>();

        List<WriterProcess> readProcessList = new ArrayList<>();
        List<RemovalProcess> removalProcessList = new ArrayList<>();

        // Using AltingChannel, so we could use Guards
        List<AltingChannelInput> inputControllerRead = new ArrayList<>();
        List<AltingChannelInput> inputControllerRemove = new ArrayList<>();

        // Parallel process
        for (int i = 0; i < READ_GROUP_COUNT; i++) {
            One2OneChannel channel = Channel.one2one();
            readChannelOutput.add(channel.out());
            One2OneChannel controller = Channel.one2one();
            inputControllerRead.add(controller.in());
            WriterProcess process = new WriterProcess(channel.in(), controller.out());
            readProcessList.add(process);
        }

        for (int i = 0; i < REMOVE_GROUP_COUNT; i++) {
            One2OneChannel channel = Channel.one2one();
            removeChannelOutput.add(channel.out());
            One2OneChannel controller = Channel.one2one();
            inputControllerRemove.add(controller.in());
            RemovalProcess process = new RemovalProcess(channel.in(), controller.out());
            removalProcessList.add(process);
        }

        Process_00 mainProcess = new Process_00(filePath, readChannelOutput, removeChannelOutput, resultChannel.in());

        B = new ProcessController(inputControllerRead, inputControllerRemove, resultChannel.out());

        Parallel parallel = new Parallel();
        parallel.addProcess(mainProcess);
        for (WriterProcess p : readProcessList) {
            parallel.addProcess(p);
        }
        for (RemovalProcess p : removalProcessList) {
            parallel.addProcess(p);
        }
        parallel.addProcess(B);
        parallel.run();

        for(RemoveItem item: B.getMergedList()){
            System.out.printf(item.name + " " + item.count);
        }
        System.out.println(B.getMergedList().size());
    }

    class ReadItem {
        String name;
        int count;
        double price;

        public ReadItem(String name, int count, double price) {
            this.name = name;
            this.count = count;
            this.price = price;
        }
    }

    class RemoveItem {
        String name;
        int count;

        public RemoveItem(String name, int count) {
            this.name = name;
            this.count = count;
        }
    }

    class Process_00 implements CSProcess {

        private String filePath;
        private List<List<ReadItem>> readGroupList = new ArrayList<>();
        private List<List<RemoveItem>> removeGroupList = new ArrayList<>();
        // Send read items
        private List<ChannelOutput> outputReadChannelList = new ArrayList<>();
        // Send removal items
        private List<ChannelOutput> outputRemovalChannelList = new ArrayList<>();
        // Receive mergedList list via this
        private ChannelInput inputResult;

        public Process_00(
                String filePath,
                List<ChannelOutput> readChannelList,
                List<ChannelOutput> removeChannelList,
                ChannelInput r) {
            this.filePath = filePath;
            this.outputReadChannelList = readChannelList;
            this.outputRemovalChannelList = removeChannelList;
            this.inputResult = r;
        }
        @Override
        public void run() {
            readFile(filePath, readGroupList, removeGroupList);

            for (int i = 0; i < readGroupList.size(); i++) {
                outputReadChannelList.get(i).write(readGroupList.get(i));
            }

            for (int i = 0; i < removeGroupList.size(); i++) {
                outputRemovalChannelList.get(i).write(removeGroupList.get(i));
            }

            try {
                printInitial(readGroupList, removeGroupList);
                inputResult.read(); // Wait for mergedList list
                printResults();
            } catch (IOException ex) {

            }
        }

        private void readFile(String path, List<List<ReadItem>> readGroupList, List<List<RemoveItem>> removeGroupList) {
            File file = new File(path);
            try {
                Scanner sc = new Scanner(file);
                if (sc.hasNextLine()) {
                    for (int j = 0; j < Main.READ_GROUP_COUNT; j++) {
                        List<ReadItem> T = new ArrayList<>();
                        sc.next(); // skip
                        int sk = sc.nextInt();
                        for (int k = 0; k < sk; k++) {
                            String name = sc.next();
                            int count = sc.nextInt();
                            ReadItem st = new ReadItem(name, count, sc.nextDouble());
                            T.add(st);
                        }
                        readGroupList.add(T);
                    }
                    for (int j = 0; j < Main.REMOVE_GROUP_COUNT; j++) {
                        List<RemoveItem> P = new ArrayList<>();
                        sc.next(); // skip
                        int sk = sc.nextInt();
                        for (int k = 0; k < sk; k++) {
                            String name = sc.next();
                            int count = sc.nextInt();
                            RemoveItem darb = new RemoveItem(name, count);
                            P.add(darb);
                        }
                        removeGroupList.add(P);
                    }
                }
                sc.close();
            } catch (FileNotFoundException e) {

            }
        }

        private void printInitial(List<List<ReadItem>> readGroupList, List<List<RemoveItem>> removeGroupList) throws IOException {
            List<String> lineList = new ArrayList<>();

            lineList.add("readGroupList:");
            for (List<ReadItem> itemGroup: readGroupList) {
                for (ReadItem item : itemGroup) {
                    lineList.add(item.name + " " + item.count + " " + item.price);
                }
            }
            lineList.add("removeGroupList:");
            for (List<RemoveItem> itemGroup : removeGroupList) {
                for (RemoveItem item : itemGroup) {
                    lineList.add(item.name + " " + item.count);
                }
            }
            Files.write(new File(RESULTS).toPath(), lineList, StandardOpenOption.CREATE);
        }

        private void printResults() throws IOException {
            List<RemoveItem> duom = B.getMergedList();
            List<String> lineList = new ArrayList<>();
            lineList.add("Results:");
            for (RemoveItem d : duom) {
                lineList.add(d.name + " " + d.count);
            }
            Files.write(new File(RESULTS).toPath(), lineList, StandardOpenOption.APPEND);
        }
    }

    class WriterProcess implements CSProcess {

        private List<ReadItem> list = new ArrayList<>();
        private ChannelOutput outputData; // To Process 00
        private ChannelInput inputData; // From Process 00

        public WriterProcess(ChannelInput in, ChannelOutput out) {
            this.inputData = in;
            this.outputData = out;
        }

        @Override
        public void run() {
            list = (List<ReadItem>) inputData.read();

            for (ReadItem item: list) {
                RemoveItem sortedItem = new RemoveItem(item.name, item.count);
                outputData.write(sortedItem); // Send to Process 00 for adding
            }
            readProcessDoneCount++;
        }
    }

    class RemovalProcess implements CSProcess {

        private List<RemoveItem> list = new ArrayList<>();
        private ChannelOutput outputData; // To Procesas 00
        private ChannelInput inputData; // From Procesas 00

        public RemovalProcess(ChannelInput in, ChannelOutput out) {
            this.inputData = in;
            this.outputData = out;
        }

        @Override
        public void run() {
            list = (List<RemoveItem>) inputData.read();
            while (Main.failedRemoveCount < 1000 && !list.isEmpty()) {
                for (RemoveItem p : list) {
                    outputData.write(p);
                }
            }
        }
    }

    class ProcessController implements CSProcess {
        private List<RemoveItem> mergedList;
        private List<AltingChannelInput> inputReadChannelList = new ArrayList<>();
        private List<AltingChannelInput> inputRemovalChannelList = new ArrayList<>();
        private ChannelOutput resultChannel;

        public ProcessController(List<AltingChannelInput> k1, List<AltingChannelInput> k2, ChannelOutput rez) {
            this.inputReadChannelList = k1;
            this.inputRemovalChannelList = k2;
            this.resultChannel = rez;
            this.mergedList = new ArrayList<>();
        }

        @Override
        public void run() {
            int readChannelCount = inputReadChannelList.size();
            int removalChannelCount = inputRemovalChannelList.size();
            int size = readChannelCount + removalChannelCount + 1;

            Guard[] G = new Guard[size];

            for (int i = 0; i < readChannelCount; i++) {
                G[i] = inputReadChannelList.get(i);
            }

            for (int i = 0; i < removalChannelCount; i++) {
                G[i + readChannelCount] = inputRemovalChannelList.get(i);
            }

            CSTimer timer = new CSTimer();
            G[size - 1] = timer;

            timer.setAlarm(timer.read() + 1000);

            final Alternative alt = new Alternative(G);

            boolean isRunning = true;
            while (isRunning) {
                int liveChannelIndex = alt.fairSelect();
                if (liveChannelIndex < readChannelCount) {
                    RemoveItem item = (RemoveItem) inputReadChannelList.get(liveChannelIndex).read();
                    insertItem(item);
                } else if (liveChannelIndex < readChannelCount + removalChannelCount && liveChannelIndex >= readChannelCount) {
                    RemoveItem item = (RemoveItem) inputRemovalChannelList.get(liveChannelIndex - readChannelCount).read();
                    removeItem(item);
                } else {
                    isRunning = false;
                }
            }
            resultChannel.write(null);
        }

        public List<RemoveItem> getMergedList() {
            return mergedList;
        }

        public void insertItem(RemoveItem d) {
            int where = findInsertionIndex(d);
            if (mergedList.size() == 0) {
                mergedList.add(d);
                return;
            }
            if (where == mergedList.size()) {
                mergedList.add(d);
                return;
            }
            if (mergedList.get(where).name.equals(d.name)) {
                mergedList.get(where).count += d.count;
            } else {
                mergedList.add(where, d);
            }
        }

        private int findInsertionIndex(RemoveItem d) {
            for (int i = 0; i < mergedList.size(); i++) {
                int compare = d.name.compareTo(mergedList.get(i).name);
                if (compare <= 0) {
                    return i;
                }
            }
            return 0;
        }

        public void removeItem(RemoveItem d) {
            if (contains(d)) {
                for (int i = 0; i < mergedList.size(); i++) {
                    if (d.name.equals(mergedList.get(i).name)) {
                        if (d.count == mergedList.get(i).count) {
                            mergedList.remove(i);
                        } else if (d.count < mergedList.get(i).count) {
                            mergedList.get(i).count -=  d.count;
                            return;
                        }
                    }
                }
            }
        }

        private boolean contains(RemoveItem d) {
            for (int i = 0; i < mergedList.size(); i++) {
                if (d.name.equals(mergedList.get(i).name) && d.count <= mergedList.get(i).count) {
                    return true;
                }
            }
            failedRemoveCount++;
            return false;
        }
    }
}
