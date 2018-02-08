package util;

import com.sibat.traffic.GlobalInfo;

import java.io.*;

/**
 * Created by User on 2017/5/18.
 * 输出处理结果到文件
 */
public class WriteFile {

    /**
     * B方法追加文件：使用FileWriter
     */
    public static void appendMethodB(String fileName, String content) {
        try {
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            FileWriter writer = new FileWriter(fileName, true);
            writer.write(content+"\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static BufferedWriter initBufferedWriter(String writeFileName, String outPath) {
        BufferedWriter bw = null;
        try {
            String path = outPath + "/" + writeFileName + ".txt";
            File folder = new File(outPath);
            File file = new File(path);
            if (!folder.exists()) {
                folder.mkdirs();
            }
            if (!file.exists()) {
                file.createNewFile();
            }

            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
            bw = new BufferedWriter(osw);// 把filewriter的写法写成FileOutputStream形式
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bw;
    }

    public static void writeToFile(String content, BufferedWriter bw) throws IOException {
        try {
//            synchronized (bw) {//防止线程安全
                bw.write(content);
                bw.newLine();
                bw.flush();
//            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void bak_writeToFile(String content ) throws IOException {
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        String fileName = GlobalInfo.writeFileName;
        String rootPath = GlobalInfo.outPath;
        String path = rootPath +"/"+ fileName + ".txt";
        File folder = new File(rootPath);
        File file = new File(path);
        if (!folder.exists()) {
            folder.mkdirs();
        }
        if(!file.exists()){
            file.createNewFile();
        }
        try {
            fos = new FileOutputStream(file, true);
            bos = new BufferedOutputStream(fos);
            content=content+"\n";
            bos.write(content.getBytes());
            bos.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
