import jdk.nashorn.internal.ir.WhileNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestHdfsClient {

    final String defaultDataNodeUrl = "hdfs://had002:9000";
    final String defaultUsr = "lee";
    final int defaultReplication = 3;

    /**
     * 测试配置
     * @throws IOException
     */
    @Test
    public void testConfigure() throws IOException {
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS", defaultDataNodeUrl);
        configuration.set("dfs.replication",String.valueOf(defaultReplication));

        FileSystem fs = FileSystem.get(configuration);
        System.out.println(fs.toString());
    }

    /**
     * 测试上传
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testHdfs() throws IOException, URISyntaxException, InterruptedException {
        System.out.println("ends");

        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://had002:9000"),configuration, "lee");

        // 2 创建要上传文件所在的本地路径
        Path src = new Path("D:\\Test\\dat.txt");

        // 3 创建要上传到hdfs的目标路径
        Path dst = new Path("hdfs://had002:9000/user/lee/dat.txt");

        // 4 拷贝文件
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

    /**
     * 测试下载
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDownload() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI(defaultDataNodeUrl),configuration,defaultUsr);

        Path dst = new Path("D:\\Test\\dat_cp.txt");
        Path src = new Path("hdfs://had002:9000/user/lee/dat.txt");

        fs.copyToLocalFile(src,dst);
        fs.close();
    }

    /**
     * 测试创建文件夹
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testMkDir() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI(defaultDataNodeUrl),configuration,defaultUsr);

        Path src = new Path("hdfs://had002:9000/test_mkdir");

        fs.mkdirs(src);
        fs.close();
    }

    /**
     * 测试删除
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDeleteDir() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI(defaultDataNodeUrl),configuration,defaultUsr);

        Path src = new Path("hdfs://had002:9000/test_mkdir");

        fs.delete(src,true);
        fs.close();
    }

    /**
     * 测试重命名
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI(defaultDataNodeUrl),configuration,defaultUsr);

        Path src = new Path("hdfs://had002:9000/user");
        Path dst = new Path("hdfs://had002:9000/user_test");

        fs.rename(src,dst);
        fs.close();
    }

    /**
     * 测试获取文件信息
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI(defaultDataNodeUrl),configuration,defaultUsr);

        Path src = new Path("hdfs://had002:9000/user_test");

        // 返回迭代器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(src,true);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for(BlockLocation bl:blockLocations){
                System.out.println("block offset : "+ bl.getOffset());
                String [] hosts = bl.getHosts();
                for (String host:hosts){
                    System.out.println(host);
                }

            }
            System.out.println("====Next=======================");
        }
        fs.close();

    }

    @Test
    public void testUploadByStream() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI(defaultDataNodeUrl),configuration,defaultUsr);
        String src = "D:\\Test\\testJson.json";
        String dst = "hdfs://had002:9000/user";


        FileInputStream inStream = new FileInputStream(new File(src));

        FSDataOutputStream outputStream = fs.create(new Path(dst));

        //对接流
        try{
            IOUtils.copyBytes(inStream,outputStream,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outputStream);
        }

        fs.close();
    }

}