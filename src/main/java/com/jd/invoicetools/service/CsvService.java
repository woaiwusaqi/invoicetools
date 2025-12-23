package com.jd.invoicetools.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class CsvService {

    public ByteArrayInputStream filterColumns(InputStream input, String col1Name, String col2Name) throws IOException {
        // 先把输入读为字节，这样可以尝试不同编码进行解析（先 UTF-8，再回退到 GBK）
        byte[] inputBytes;
        try (ByteArrayOutputStream tmp = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int r;
            while ((r = input.read(buf)) != -1)
                tmp.write(buf, 0, r);
            inputBytes = tmp.toByteArray();
        }

        String[] tryCharsets = new String[] { StandardCharsets.UTF_8.name(), "GBK" };
        byte[] lastResult = null;

        for (String cs : tryCharsets) {
            try (Reader reader = new InputStreamReader(new ByteArrayInputStream(inputBytes), cs);
                    CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

                Map<String, Integer> headerMap = parser.getHeaderMap();
                List<String> headersList = parser.getHeaderNames();

                String foundCol1 = findHeader(headerMap.keySet(), col1Name);
                String foundCol2 = findHeader(headerMap.keySet(), col2Name);

                if (foundCol1 == null) {
                    for (String h : headersList) {
                        if (h == null)
                            continue;
                        String norm = h.replaceAll("\\s+", "").toLowerCase();
                        if (norm.contains("订单号")) {
                            foundCol1 = h;
                            break;
                        }
                    }
                }
                if (foundCol1 == null) {
                    for (String h : headersList) {
                        if (h == null)
                            continue;
                        String norm = h.replaceAll("\\s+", "").toLowerCase();
                        if (norm.contains("订单") && !norm.contains("客户") && !norm.contains("账号")) {
                            foundCol1 = h;
                            break;
                        }
                    }
                }
                if (foundCol2 == null) {
                    for (String h : headersList) {
                        if (h == null)
                            continue;
                        String norm = h.replaceAll("\\s+", "").toLowerCase();
                        if (norm.contains("订单机构") || norm.contains("机构")) {
                            foundCol2 = h;
                            break;
                        }
                    }
                }

                int idx1 = 1; // 订单号索引
                int idx2 = 10; // 订单机构索引

                boolean useIndexMode = false;
                if (headersList == null || headersList.isEmpty()) {
                    useIndexMode = true;
                } else if (foundCol1 == null || foundCol2 == null) {
                    if (headersList.size() > Math.max(idx1, idx2))
                        useIndexMode = true;
                }

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                out.write(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF });

                // 收集所有记录
                java.util.List<String[]> rows = new java.util.ArrayList<>();
                boolean sawReplacement = false;
                for (CSVRecord record : parser) {
                    String a = "";
                    String b = "";
                    if (useIndexMode) {
                        if (record.size() > idx1)
                            a = record.get(idx1);
                        if (record.size() > idx2)
                            b = record.get(idx2);
                    } else {
                        if (foundCol1 != null)
                            a = record.get(foundCol1);
                        if (foundCol2 != null)
                            b = record.get(foundCol2);
                    }
                    if (containsReplacementChar(a) || containsReplacementChar(b))
                        sawReplacement = true;
                    rows.add(new String[] { a, b });
                }

                // 按订单机构分组并在组内对订单号排序
                java.util.TreeMap<String, java.util.List<String>> groups = new java.util.TreeMap<>();
                for (String[] r : rows) {
                    String order = r[0] == null ? "" : r[0];
                    String org = r[1] == null ? "" : r[1];
                    groups.computeIfAbsent(org, k -> new java.util.ArrayList<>()).add(order);
                }

                for (java.util.List<String> list : groups.values()) {
                    list.sort((o1, o2) -> {
                        if (o1 == null) return (o2 == null) ? 0 : -1;
                        if (o2 == null) return 1;
                        try {
                            long n1 = Long.parseLong(o1.replaceAll("\\D", ""));
                            long n2 = Long.parseLong(o2.replaceAll("\\D", ""));
                            return Long.compare(n1, n2);
                        } catch (Exception e) {
                            return o1.compareTo(o2);
                        }
                    });
                }

                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
                        CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(col1Name, col2Name))) {

                    for (java.util.Map.Entry<String, java.util.List<String>> e : groups.entrySet()) {
                        String org = e.getKey();
                        for (String order : e.getValue()) {
                            printer.printRecord(order, org);
                        }
                    }
                    printer.flush();
                }

                lastResult = out.toByteArray();
                if (!sawReplacement) {
                    return new ByteArrayInputStream(lastResult);
                }
                // else try next charset (GBK)
            }
        }

        // 如果所有尝试都出现替换字符，返回最后一次结果
        if (lastResult != null)
            return new ByteArrayInputStream(lastResult);
        return new ByteArrayInputStream(new byte[0]);
    }

    

    private String findHeader(Set<String> headers, String target) {
        for (String h : headers) {
            if (h != null && h.trim().equalsIgnoreCase(target.trim())) {
                return h;
            }
        }
        return null;
    }

    private boolean containsReplacementChar(String s) {
        if (s == null)
            return false;
        return s.indexOf('\uFFFD') >= 0;
    }
}
