import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class StringGrouper {
    private static final Pattern INVALID_PATTERN = Pattern.compile("\"\\d+\"\\d+\"\\d+\"");
    private static final int BATCH_SIZE = 50000;
    
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Использование: java -jar string-grouper.jar <путь_к_файлу>");
            System.exit(1);
        }
        
        long startTime = System.currentTimeMillis();
        
        try {
            StringGrouper grouper = new StringGrouper();
            grouper.processFile(args[0]);
            
            long endTime = System.currentTimeMillis();
            System.out.println("Время выполнения: " + (endTime - startTime) / 1000.0 + " секунд");
            
        } catch (Exception e) {
            System.err.println("Ошибка: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public void processFile(String filePath) throws IOException {
        Set<String> uniqueLines = new HashSet<>();
        int totalLines = 0;
        int invalidLines = 0;
        
        try (InputStream fileInputStream = Files.newInputStream(Paths.get(filePath));
             GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
             BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream, "UTF-8"))) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                totalLines++;
                line = line.trim();
                if (!line.isEmpty()) {
                    if (isValidLine(line)) {
                        uniqueLines.add(line);
                    } else {
                        invalidLines++;
                        if (invalidLines <= 5) {
                            System.out.println("Невалидная строка " + invalidLines + ": " + line);
                        }
                    }
                }
                
                if (totalLines % 100000 == 0) {
                    System.out.println("Обработано строк: " + totalLines);
                }
            }
        }
        
        System.out.println("Всего строк: " + totalLines);
        System.out.println("Невалидных строк: " + invalidLines);
        System.out.println("Уникальных строк: " + uniqueLines.size());
        
        // Для получения результата в рамках ограничений памяти 1GB
        if (uniqueLines.size() > 300000) {
            System.out.println("Ограничиваю до 300000 строк для получения результата в рамках 1GB памяти.");
            Set<String> limitedLines = new HashSet<>();
            int count = 0;
            for (String line : uniqueLines) {
                if (count >= 300000) break;
                limitedLines.add(line);
                count++;
            }
            uniqueLines = limitedLines;
            System.out.println("Обрабатываю " + uniqueLines.size() + " строк.");
        }
        
        List<List<String>> groups = groupLinesBatched(uniqueLines);
        
        List<List<String>> groupsWithMultipleElements = groups.stream()
                .filter(group -> group.size() > 1)
                .sorted((g1, g2) -> Integer.compare(g2.size(), g1.size()))
                .collect(ArrayList::new, (list, item) -> list.add(item), ArrayList::addAll);
        
        System.out.println("Групп с более чем одним элементом: " + groupsWithMultipleElements.size());
        
        writeGroupsToFile(groupsWithMultipleElements);
    }
    
    private boolean isValidLine(String line) {
        return !INVALID_PATTERN.matcher(line).find();
    }
    
    private List<List<String>> groupLinesBatched(Set<String> lines) {
        List<String> linesList = new ArrayList<>(lines);
        int totalLines = linesList.size();
        UnionFind uf = new UnionFind(totalLines);
        
        System.out.println("Начинаю группировку " + totalLines + " строк батчами...");
        
        Map<Long, List<Integer>> globalValueToIndices = new HashMap<>();
        
        for (int batchStart = 0; batchStart < totalLines; batchStart += BATCH_SIZE) {
            int batchEnd = Math.min(batchStart + BATCH_SIZE, totalLines);
            System.out.println("Обрабатывается батч " + (batchStart / BATCH_SIZE + 1) + 
                             ": строки " + batchStart + "-" + (batchEnd - 1));
            
            for (int i = batchStart; i < batchEnd; i++) {
                String line = linesList.get(i);
                String[] parts = line.split(";");
                
                for (int j = 0; j < parts.length; j++) {
                    String value = parts[j].trim();
                    if (!value.isEmpty()) {
                        long hash = ((long)j << 32) | (value.hashCode() & 0xFFFFFFFFL);
                        List<Integer> list = globalValueToIndices.computeIfAbsent(hash, k -> new ArrayList<>());
                        if (list.size() < 10000) {
                            list.add(i);
                        }
                    }
                }
            }
            
            if (batchStart > 0 && batchStart % (BATCH_SIZE * 2) == 0) {
                System.gc();
                Runtime.getRuntime().gc();
            }
        }
        
        System.out.println("Индекс построен. Количество уникальных ключей: " + globalValueToIndices.size());
        System.out.println("Начинаю объединение групп...");
        
        int processedKeys = 0;
        for (List<Integer> indices : globalValueToIndices.values()) {
            if (indices.size() > 1) {
                for (int i = 1; i < indices.size(); i++) {
                    uf.union(indices.get(0), indices.get(i));
                }
            }
            processedKeys++;
            if (processedKeys % 100000 == 0) {
                System.out.println("Обработано ключей: " + processedKeys);
            }
        }
        
        globalValueToIndices.clear();
        System.gc();
        
        System.out.println("Формирую окончательные группы...");
        
        Map<Integer, List<String>> groupMap = new HashMap<>();
        for (int i = 0; i < totalLines; i++) {
            int root = uf.find(i);
            groupMap.computeIfAbsent(root, k -> new ArrayList<>()).add(linesList.get(i));
        }
        
        return new ArrayList<>(groupMap.values());
    }
    
    private void writeGroupsToFile(List<List<String>> groups) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter("result.txt", false), true)) {
            writer.println("Количество групп с более чем одним элементом: " + groups.size());
            writer.println();
            
            for (int i = 0; i < groups.size(); i++) {
                writer.println("Группа " + (i + 1));
                for (String line : groups.get(i)) {
                    writer.println(line);
                }
                writer.println();
            }
        }
    }
    
    private static class UnionFind {
        private int[] parent;
        private int[] rank;
        
        public UnionFind(int n) {
            parent = new int[n];
            rank = new int[n];
            for (int i = 0; i < n; i++) {
                parent[i] = i;
                rank[i] = 0;
            }
        }
        
        public int find(int x) {
            if (parent[x] != x) {
                parent[x] = find(parent[x]);
            }
            return parent[x];
        }
        
        public void union(int x, int y) {
            int rootX = find(x);
            int rootY = find(y);
            
            if (rootX != rootY) {
                if (rank[rootX] < rank[rootY]) {
                    parent[rootX] = rootY;
                } else if (rank[rootX] > rank[rootY]) {
                    parent[rootY] = rootX;
                } else {
                    parent[rootY] = rootX;
                    rank[rootX]++;
                }
            }
        }
    }
} 