/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HGMD;

import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.bior.publishers.Cosmic.CosmicPublisher;
import edu.mayo.bior.utils.GetBasesUtil;
import edu.mayo.bior.utils.SQLParser;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.ReplaceAllPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.history.HCutPipe;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import net.minidev.json.JSONObject;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import org.h2.jdbcx.JdbcDataSource;


/**
 *
 * @author m102417
 */
public class HGMDPublisher {
    private SQLParser sqlp = new SQLParser(true);
    public static void main(String[] args) throws IOException, SQLException{
        HGMDPublisher pub = new HGMDPublisher();
        String hg19file = "/data/hgmd/2012_3/pro/setup/data/pro/hg19_coords.txt.gz";
        String schemaFile = "/data/hgmd/2012_2/table_schema.txt";
        //pub.ParseSQL(schemaFile);
        //pub.loadHG19cords(hg19file);
        pub.setupTmpH2(schemaFile, "/tmp");
        pub.loadTable("mutation", "/data/hgmd/2012_3/pro/setup/data/pro/");
    }
    Connection conn;
    public void setupTmpH2(String schemaFile, String tmpdir) throws SQLException, IOException{  
        cleanup(tmpdir);
        List<String> lines = sqlp.loadFileToMemory(schemaFile);       
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:"+tmpdir+"/hdmd;MODE=MYSQL");
        ds.setUser("me");
        ds.setPassword("none");
        conn = ds.getConnection();
        for(int i=0; i<lines.size();i++){
            int start = sqlp.getCreateLine(lines, i);
            int end = sqlp.getCloseLine(lines, i);
            List<String> sublines = lines.subList(start, end);
            createTable(sublines,conn);
            i = end+1;
        }

    }
    
    public void cleanup(String root){
        File f = new File(root + "/hdmd.h2.db");
        File f2 = new File(root + "/hdmd.trace.db");
        if(f.exists()){
            f.delete();
        }
        if(f2.exists()){
            f2.delete();
        }
    }
    
    /**
     * creates the H2 table given the SQL statement (as a list of lines)
     * @param lines the create statement
     * @param conn  H2 connection
     * @throws SQLException 
     */
    public void createTable(List<String> lines, Connection conn) throws SQLException{
        Statement statement = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        for(String line :lines){
            if(line.contains("ENGINE")) break;
            else {
                sb.append(line.replaceAll("ZEROFILL", "").replaceAll("ENUM.*", "VARCHAR(128),").replaceAll("full", "afull"));
                sb.append("\n");
            }
        }
        sb.append(")");
        //sb.append("CREATE TABLE example(id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),name VARCHAR(30), age INT)");
        statement.execute(sb.toString());
        statement.close();
    }
    
    public void loadTable(String tablename, String filesDir) throws SQLException{
        Statement statement = conn.createStatement();
        Pipeline p = new Pipeline(
                new CatPipe(),
                new PrintPipe()
                );
        p.setStarts(Arrays.asList(filesDir + tablename + ".txt.gz"));
        for(int i=0; p.hasNext(); i++){
            String s = (String) p.next();
        }
        statement.close();
    }
    
    /**
     * given a file containing MYSQL, parse it and construct a hashmap of <TableName,Injector> pairs
     * @param sqlFile
     * @return
     * @throws IOException 
     */
    public HashMap<String,ColumnInjector> ParseSQL(String sqlFile) throws IOException{  
        System.out.println("Parsing SQL: " + sqlFile);
        HashMap<String,ColumnInjector> hm = sqlp.getInjectorsFromSQLFile(sqlFile);
        return hm;
    }
    

    private class Alleles {
        public String ref;
        public List<String> alt; 
        public Alleles(){
            alt = new ArrayList<String>();
        }
    }
    
   /** 
     * @param base - the string representation of the mutation in hgmd
     * @return 
     */
    public Alleles translateMutation(String base){
        Alleles alleles = new Alleles();
        String l = base.replaceAll("a|t|g|c|n|x", "");
            String[] split = l.split("-");
            for(int i=0; i<split[0].length(); i++){
                if(split[0].charAt(i) != split[1].charAt(i)){
                    alleles.alt.add(Character.toString(split[1].charAt(i)).toUpperCase());
                    alleles.ref = (Character.toString(split[0].charAt(i)).toUpperCase());
                }
            } 
        return alleles;
    }
    
    /**
     * 
     * @param insertion - the string representation of the insertion in hgmd
     * @return 
     */
    GetBasesUtil baseu = new GetBasesUtil();
    public Alleles translateInsertion(String insertion){
        Alleles alleles = new Alleles();
        return alleles;
    }
    

    
        /*
     * INSERTION
		Micro-insertions (20 bp or less). Specific fields are:
		INSERTION. Insertions are presented in terms of the inserted bases in lower case plus, in upper case, 10 bp DNA sequence 
		flanking both sides of the lesion. 
		The numbered codon from the AMINO field is preceded in the given sequence by the caret character (^).
		AMINO. The number of the codon referenced in the INSERTION field by the caret character (^).
		NUCLEOTIDE. Nucleotide number (as found in the corresponding report). DEPRECIATED.
     * 
    CREATE TABLE insertion (
            0 disease VARCHAR(125),
            1 gene VARCHAR(10),
            2 insertion VARCHAR(65),
            3 codon INT,
            4 nucleotide VARCHAR(10),
            5 tag ENUM('DP','FP','DFP','DM','DM?','FTV'),
            6 author VARCHAR(25),
            7 journal VARCHAR(10),
            8 fullname VARCHAR(50),
            9 vol VARCHAR(4),
            10 page VARCHAR(10),
            11 year YEAR(4),
            12 pmid VARCHAR(8),
            13 comments VARCHAR(125),
            14 acc_num VARCHAR(10) UNIQUE NULL,
            15 new_date DATE, PRIMARY KEY (gene, insertion, codon, nucleotide)
    ) ENGINE = MyISAM CHARSET=utf8
    */
//    String[] fields = {"TYPE", "ONELINER", "refAlleleFWD", "altAlleleFWD"};
//    public static class insertiontranslate implements PipeFunction<String[],Variant> {
//        public Variant compute(String[] s) {
//            Variant variant = new Variant();
//            variant.setNspace(NSpace.HGMD);
//            variant.setType("insertion");
//            variant.setVersion(version);
//            variant.setId(s[14]);
//            variant.setOneLiner(s[2]);
//            variant.setDescription(s[13]);
//            variant.setTag(ThriftObjectUtils.computeVariantTag(s[5]));
//            variant.setDescription(s[13]);
//            Map properties = new HashMap();
//            properties.put("codon", s[3]);
//            variant.setProperties(properties);
//            String tmp2 = s[2].replaceAll("\\^", "");
//            String l = tmp2.replaceAll("A|T|G|C|N|X", "");
//            //System.out.println(l);
//            ArrayList altal = new ArrayList();
//            altal.add(l.toUpperCase());
//            variant.setAltAlleleFWD(altal);
//            variant.setRefAlleleFWD("-");
//            //System.out.println(v.toString());
//            return variant;
//        }
//    }
    
    


    
}
