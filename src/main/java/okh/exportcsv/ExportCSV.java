/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package okh.exportcsv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

/**
 *
 * @author okahana
 */
public class ExportCSV {

    /**
     * @param args the command line arguments
     * @throws org.apache.commons.cli.ParseException
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) throws ParseException, ClassNotFoundException, SQLException {
        Options options = new Options();
        options.addRequiredOption("h", "host", true, "ホスト名");
        options.addOption("P", "port", true, "ポート番号(デフォルト:1521)");
        options.addRequiredOption("s", "sid", true, "SIDまたはサービス名");
        options.addRequiredOption("u", "user", true, "ユーザー名");
        options.addRequiredOption("p", "password", true, "パスワード");
        options.addOption("b", "outputbuffersize", true, "出力バッファサイズ(単位:バイト, デフォルト:65536)");
        options.addOption("f", "fetchsize", true, "一度にFETCHする行数(デフォルト:1000)");
        options.addOption("m", "insertmode", true, "SQL*Loaderのinsertモード(デフォルトはtruncate)");
        options.addOption("o", "options", true, "SQL*LoaderのコントロールファイルのOPTIONSに指定するオプション。\n例:direct=true,multithreading=true");
        options.addOption("z", "zipfile", true, "出力ファイル(CSV,CTL,BAT)を格納するZIPファイル名");
        options.addOption("c", "compresslevel", true, "ZIP圧縮レベル(1-9)");
        options.addOption(Option.builder("t").longOpt("tables").desc("テーブル名を,で区切って複数指定(指定なしなら全テーブル)").hasArgs().valueSeparator(',').build());
        options.addOption(Option.builder("x").longOpt("exclude").desc("除外するテーブル名のパターン(LIKE形式)").hasArgs().valueSeparator(',').build());
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (MissingOptionException e) {
            System.out.println(e.getMessage());
            HelpFormatter hf = new HelpFormatter();
            hf.setOptionComparator(null);
            hf.printHelp("ExportCSV", options);
            return;
        }
        ExportCSV exportCSV = new ExportCSV(cmd);
        try {
            exportCSV.run();
        } catch (IOException ex) {
            Logger.getLogger(ExportCSV.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    String host;
    int port;
    String sid;
    String user;
    String password;
    String[] tables;
    String[] excludes;
    Connection db;
    int outputBufferSize;
    int fetchSize;
    String mode;
    String options;
    String zipName;
    int compressLevel;
    ExportCSV(CommandLine cmd) {
        host = cmd.getOptionValue("host");
        port = 1521;
        try {
            if (cmd.hasOption("host")) {
                port = Integer.parseInt(cmd.getOptionValue("port"));
            }
        } catch (NumberFormatException e) {
        }
        sid = cmd.getOptionValue("sid");
        user = cmd.getOptionValue("user");
        password = cmd.getOptionValue("password");
        tables = cmd.getOptionValues("tables");
        excludes = cmd.getOptionValues("exclude");
        outputBufferSize = 65536;
        try {
            if (cmd.hasOption("outputbuffersize")) {
                outputBufferSize = Integer.parseInt(cmd.getOptionValue("outputbuffersize"));
            }
        } catch (NumberFormatException e) {
        }
        fetchSize = 1000;
        try {
            if (cmd.hasOption("fetchsize")) {
                fetchSize = Integer.parseInt(cmd.getOptionValue("fetchsize"));
            }
        } catch (NumberFormatException e) {
        }
        if (cmd.hasOption("insertmode")) {
            mode = cmd.getOptionValue("insertmode");
        } else {
            mode = "truncate";
        }
        options = cmd.getOptionValue("options");
        zipName = cmd.getOptionValue("zipfile");
        compressLevel = 5;
        if (cmd.hasOption("compresslevel")) {
            try {
                compressLevel = Integer.parseInt(cmd.getOptionValue("compresslevel"));
            }
            catch (NumberFormatException e) {}
        }
    }

    ZipArchiveOutputStream zip;
    void run() throws ClassNotFoundException, SQLException, IOException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        try {
            db = DriverManager.getConnection("jdbc:oracle:thin:@" + host + ":" + port + ":" + sid, user, password);
        }
        catch (SQLException e) {
            System.out.println("ログインに失敗しました。");
            return;
        }
        if (tables == null) {
            tables = getAllTables();
        }
        System.out.println("zipname=" + zipName);
        if (zipName != null) {
            File file = new File(zipName);
            if (file.exists()) file.delete();
            zip = new ZipArchiveOutputStream(file);
            zip.setLevel(compressLevel);
        }
        else
            zip = null;
        long t1 = System.currentTimeMillis(), bytes = 0, b = 0;
        List<String> tableList = new ArrayList<>();
        for (String table : tables) {
            b = outputCSV(table);
            if (b == -1) break;
            bytes += b;
            if (b > 0) tableList.add(table);
        }
        db.close();
        tables = (String[]) tableList.toArray(new String[tableList.size()]);
        if (bytes != 0) outputBatchFile(tables);
        if (zip != null) {
            zip.close();
        }
        t1 = (System.currentTimeMillis() - t1) / 1000;
        if (t1 != 0)
            System.out.println(tables.length + "テーブル、合計" + formatSize(bytes) + "のCSVファイルを"+ formatSecond(t1) + "で出力しました。" + formatSize(bytes/t1) + "/sec");
        else
            System.out.println(tables.length + "テーブル、合計" + formatSize(bytes) + "のCSVファイルを"+ formatSecond(t1) + "で出力しました。");
    }
    static final String CRLF = System.lineSeparator();
    static final DecimalFormat NUMBER_FORMAT = new DecimalFormat("#,##0");
    static final DecimalFormat PERCENT_FORMAT = new DecimalFormat("#,##0.00%");
    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
    long outputCSV(String table) throws SQLException, IOException {
        long rowCount = 0, bytes = 0;
        PreparedStatement stmt;
        stmt = db.prepareStatement("select count(*) from " + table);
        try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                rowCount = rs.getLong(1);
                System.out.println(table + " " + NUMBER_FORMAT.format(rowCount) + " 行");
                if (rowCount == 0) {
                    stmt.close();
                    return 0;
                }
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("ORA-00942")) {
                System.out.println(table + " テーブルは存在しません。処理を中止します。");
                stmt.close();
                return -1;
            }
        }

        stmt = db.prepareStatement("select * from " + table);
        long t1 = System.currentTimeMillis(), t2 = System.currentTimeMillis();
        final long interval = 3000;
        try (ResultSet rs = stmt.executeQuery()) {
            rs.setFetchSize(fetchSize);
            ResultSetMetaData meta = rs.getMetaData();
            outputControlFile(table, meta);
            int colcount = meta.getColumnCount();
            String data;
            
            File file = new File(table + ".csv");
            OutputStream fw = null;
            if (zip != null) {
                zip.putArchiveEntry(new ZipArchiveEntry(file.getName()));
                fw = zip;
            }
            else {
                if (file.exists()) {
                    file.delete();
                }
                fw = new FileOutputStream(file);
            }
            StringBuilder w = new StringBuilder(1024*100);
            long row = 0, total = 0;
            while (rs.next()) {
                ++row;
                ++total;
                for (int i = 1; i <= colcount; i++) {
                    if (meta.getColumnTypeName(i).equals("DATE")) {
                        Date dt = rs.getDate(i);
                        data = dt != null ? DATE_FORMAT.format(dt) : "";
                    }
                    else if (meta.getColumnTypeName(i).equals("TIMESTAMP")) {
                        Timestamp ts = rs.getTimestamp(i);
                        if (ts != null) {
                            LocalDateTime ldt = ts.toLocalDateTime();
                            data = TIMESTAMP_FORMAT.format(ldt);
                        }
                        else data = "";
                    }
                    else {
                        try {
                            data = rs.getString(i).replace("\"", "\"\"");
                        } catch (NullPointerException e) {
                            data = "";
                        }
                    }
                    //data = rs.wasNull() ? "" : data.replace("\"", "\"\"");
                    if (i > 1) {
                        w.append(',');
                    }
                    w.append('"').append(data).append('"');
                }
                w.append(CRLF);
                if (w.length() > 1024*99) {
                    byte[] rec = w.toString().getBytes();
                    fw.write(rec);
                    bytes += rec.length;
                    w.setLength(0);
                }
                if (t1 + interval < System.currentTimeMillis()) {
                    t1 += interval;
                    System.out.println("  " + table + ": " + NUMBER_FORMAT.format(row / (interval / 1000)) + " 行/秒 " + PERCENT_FORMAT.format(rowCount != 0 ? (double)total / rowCount : 100));
                    row = 0;
                }
            }
            if (w.length() > 0) {
                byte[] rec = w.toString().getBytes();
                if (fw != null) fw.write(rec);
                bytes += rec.length;
                w.setLength(0);
                System.out.println("  " + table + ": " + NUMBER_FORMAT.format(row / (interval / 1000)) + " 行/秒 " + PERCENT_FORMAT.format(rowCount != 0 ? (double)total / rowCount : 100));
            }
            t1 = (System.currentTimeMillis() - t2) / 1000;
            if (t1 != 0)
                System.out.println(table + ": 合計 " + NUMBER_FORMAT.format(total) + " 行 (" + formatSize(bytes) + ") を " + formatSecond(t1) + "で出力しました。" + formatSize(bytes/t1) + "/sec");
            else
                System.out.println(table + ": 合計 " + NUMBER_FORMAT.format(total) + " 行 (" + formatSize(bytes) + ") を " + formatSecond(t1) + "で出力しました。");
            if (zip != null) {
                zip.closeArchiveEntry();
            }
            else {
                if (fw != null) fw.close();
            }
        }
        stmt.close();
        return bytes;
    }

    void outputControlFile(String table, ResultSetMetaData meta) throws IOException, SQLException {
        StringBuilder w = new StringBuilder();
        if (options != null) {
            w.append("options(").append(options).append(')');
            w.append(CRLF);
        }
        w.append("load data");
        w.append(CRLF);
        w.append("infile ").append(table).append(".csv");
        w.append(CRLF);
        w.append(mode);
        w.append(CRLF);
        w.append("into table ").append(table);
        w.append(CRLF);
        w.append("fields terminated by \",\" optionally enclosed by '\"'");
        w.append(CRLF);
        w.append('(');
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (i > 1) {
                w.append(',').append(CRLF);
            }
            if (meta.getColumnTypeName(i).equals("DATE")) {
                w.append(meta.getColumnName(i)).append(" date 'yyyy/MM/dd HH24:mi:ss'");
            }
            else if (meta.getColumnTypeName(i).equals("TIMESTAMP")) {
                w.append(meta.getColumnName(i)).append(" timestamp 'yyyy/MM/dd HH24:mi:ss.ff3'");
            }
            else {
                w.append(meta.getColumnName(i));
            }
            
        }
        w.append(')');
        w.append(CRLF);

        File file = new File(table + ".ctl");
        if (zip != null) {
            zip.putArchiveEntry(new ZipArchiveEntry(file.getName()));
            zip.write(w.toString().getBytes());
            zip.closeArchiveEntry();
        }
        else {
            if (file.exists()) {
                file.delete();
            }
            try (FileWriter fw = new FileWriter(file)) {
                fw.write(w.toString());
            }
        }
    }
    static final String BAT_FILE_NAME = "LOAD_ALL_CSV.BAT";
    void outputBatchFile(String[] tables) throws IOException, SQLException {
        File file = new File(BAT_FILE_NAME);
        StringWriter sw = null;
        PrintWriter w;
        if (zip == null) {
            if (file.exists()) {
                file.delete();
            }
            w = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        }
        else {
            sw = new StringWriter();
            w = new PrintWriter(sw);
        }
        w.println("@echo off");
        w.println("if \"%1\" == \"\" ( echo 引数に接続文字列^(ユーザー/パスワード@TNS名^)を指定してください。 && exit /b 1 )");
        for (String table : tables) {
            w.append("del ").append(table).append(".bad>nul 2>&1").println();
            w.append("sqlldr %1 control=").append(table).append(".ctl").println();
            w.append("if errorlevel 1 ( echo ").append(table).append("のロードに失敗しました。 && pause ) else ")
             .append("if exist ").append(table).append(".bad ( echo ").append(table).append("のロードに失敗しました。 && pause ) else ( echo ").append(table).append("のロードに成功しました。 )").println();
        }
        w.println("exit /b 0");
        w.flush();
        if (zip == null) {
            w.close();
        }
        if (zip != null) {
            zip.putArchiveEntry(new ZipArchiveEntry(BAT_FILE_NAME));
            if (sw != null) zip.write(sw.toString().getBytes());
            zip.closeArchiveEntry();;
        }
    }
    static String formatSize(long size) {
        if (size < 10000) {
            return NUMBER_FORMAT.format(size) + "byte";
        }
        if (size / 1024 < 10000) {
            return NUMBER_FORMAT.format(size / 1024) + "KB";
        }
        if (size / 1024 / 1024 < 10000) {
            return NUMBER_FORMAT.format(size / 1024 / 1024) + "MB";
        }
        return NUMBER_FORMAT.format(size / 1024 / 1024 / 1024) + "GB";
    }
    
    static String formatSecond(long sec) {
        long min = sec/60;
        sec = sec-min*60;
        
        return sec != 0 ? min + "分" + sec + "秒" : min + "分";
    }
    
    String[] getAllTables() throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("select table_name from user_tables ");
        if (excludes.length > 0) {
            sql.append("where ");
            for (int i = 0; i < excludes.length; i++) {
                if (i > 0) sql.append("and ");
                sql.append("table_name not like ? ");
            }
        }
        sql.append("order by 1");
        List<String> list = new ArrayList<>();
        PreparedStatement stmt;
        stmt = db.prepareStatement(sql.toString());
        for (int i = 0; i < excludes.length; i++) {
            stmt.setString(i+1, excludes[i]);
        }
        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                list.add(rs.getString(1));
                System.out.println(rs.getString(1));
            }
        }
        return list.isEmpty() ? null : list.toArray(new String[list.size()]);
    }
}
