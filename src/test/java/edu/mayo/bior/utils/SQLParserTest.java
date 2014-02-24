/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.utils;

import edu.mayo.bior.utils.SQLParser;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class SQLParserTest {
    
    public SQLParserTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testLoadFileToMemory() throws IOException {
        System.out.println("Test testLoadFileToMemory");
        SQLParser sqlp = new SQLParser();
        String create = "CREATE TABLE `hiSeqDepthTop5Pct` (";
        List<String> f = sqlp.loadFileToMemory("src/test/resources/testData/ucsc/hiSeqDepthTop5Pct.sql");
        for(int i = 0; i<f.size(); i++){
            System.out.println(f.get(i));
            if(i==22){
                assertEquals(create, f.get(i));
            }     
        }
        assertEquals(22, sqlp.getCreateLine(f));
        assertEquals(28, sqlp.getCloseLine(f));
    }
    
    @Test
    public void testIsFieldLine(){
        System.out.println("Test isFieldLine");
        String line = "  `bin` smallint(5) unsigned NOT NULL,";
        String noline = "  KEY `chrom` (`chrom`(14),`bin`)";
        SQLParser sqlp = new SQLParser();
        assertEquals(true, sqlp.isFieldLine(line));
        assertEquals(false, sqlp.isFieldLine(noline));
    }
    
    @Test
    public void testGetType(){
        System.out.println("Test testGetType");
        String intline = "  `bin` smallint(5) unsigned NOT NULL,";
        String stringline = "  KEY `chrom` (`chrom`(14),`bin`)";
        SQLParser sqlp = new SQLParser();
        assertEquals(JsonType.NUMBER.toString(), sqlp.getType(intline).toString()); 
        assertEquals(JsonType.STRING.toString(), sqlp.getType(stringline).toString());
        
    }
    
    @Test
    public void testGetField4Line(){
        System.out.println("Test testGetField4Line");
        SQLParser sqlp = new SQLParser();
        String s1 = "  `bin` smallint(5) unsigned NOT NULL,";
        String s2 = "  `chrom` varchar(255) NOT NULL,";
        String s3 = "  `chromStart` int(10) unsigned NOT NULL,";
        String s4 = "  `chromEnd` int(10) unsigned NOT NULL,";
        String s5 = "  KEY `chrom` (`chrom`(14),`bin`)";
        String s6 = "  journal VARCHAR(10),";
        assertEquals("bin",		sqlp.getField4Line(s1));
        assertEquals("chrom",	sqlp.getField4Line(s2));
        assertEquals("chromStart",sqlp.getField4Line(s3));
        assertEquals("chromEnd",sqlp.getField4Line(s4));
        assertEquals("chrom",	sqlp.getField4Line(s5));
        assertEquals("journal",	sqlp.getField4Line(s6));
    }
    
    @Test
    public void parseSQL() throws IOException{
        System.out.println("Test parseSQL");
        String sqlfile = "src/test/resources/testData/ucsc/coriellDelDup.sql";
        SQLParser sqlp = new SQLParser();
        List<String> loadFileToMemory = sqlp.loadFileToMemory(sqlfile);
        ColumnInjector[] inj = sqlp.getInjectorsFromSQL(loadFileToMemory);
        assertEquals(13, inj.length);
        for(int i=0; i< inj.length; i++){
            ColumnInjector in = inj[i];
            //`chrom` varchar(255) NOT NULL,
            if(i==0){
                assertEquals("chrom", in.getmKey());
            }
            //`chromStart` int(10) unsigned NOT NULL,
            if(i==1){
                assertEquals("chromStart", in.getmKey());
            }
            //`chromEnd` int(10) unsigned NOT NULL,
            if(i==2){
                assertEquals("chromEnd", in.getmKey());
            }
            //`name` varchar(255) NOT NULL,
            if(i==3){
                assertEquals("name", in.getmKey());
            }
            //`score` int(10) unsigned NOT NULL DEFAULT '0',
            if(i==4){
                assertEquals("score", in.getmKey());
            }
            //`strand` char(1) DEFAULT NULL,
            if(i==5){
                assertEquals("strand", in.getmKey());
            }
            //`thickStart` int(10) unsigned DEFAULT NULL,
            if(i==6){
                assertEquals("thickStart", in.getmKey());
            }
            //`thickEnd` int(10) unsigned DEFAULT NULL,
            if(i==7){
                assertEquals("thickEnd", in.getmKey());
            }
            //`reserved` int(10) unsigned DEFAULT NULL,
            if(i==8){
                assertEquals("reserved", in.getmKey());
            }
            //`CN_State` enum('0','1','2','3','4') NOT NULL DEFAULT '2',
            if(i==9){
                assertEquals("CN_State", in.getmKey());
            }
            //`cellType` enum('B_Lymphocyte','Fibroblast','Amniotic_fluid_cell_line','Chorionic_villus_cell_line') NOT NULL DEFAULT 'B_Lymphocyte',
            if(i==10){
                assertEquals("cellType", in.getmKey());
            }
            //`description` varchar(255) NOT NULL,
            if(i==11){
                assertEquals("description", in.getmKey());
            }
            //`ISCN` varchar(255) NOT NULL,
            if(i==12){
                assertEquals("ISCN", in.getmKey());
            }
        }
    }
    
    
    @Test
    public void parseSQL2() throws IOException{
        System.out.println("Test parseSQL2");
        String sqlfile = "src/test/resources/testData/ucsc/sibTxGraph.sql";
        SQLParser sqlp = new SQLParser();
        List<String> loadFileToMemory = sqlp.loadFileToMemory(sqlfile);
        ColumnInjector[] inj = sqlp.getInjectorsFromSQL(loadFileToMemory);
        assertEquals(19, inj.length);
        for(int i=0; i< inj.length; i++){
            ColumnInjector in = inj[i];
                        //`chrom` varchar(255) NOT NULL,
            if(i==0){
                assertEquals("bin", in.getmKey());
            }
            if(i==1){
                assertEquals("tName", in.getmKey());
            }
            if(i==2){
                assertEquals("tStart", in.getmKey());
            }
            if(i==3){
                assertEquals("tEnd", in.getmKey());
            }
            if(i==4){
                assertEquals("name", in.getmKey());
            }
            if(i==5){
                assertEquals("id", in.getmKey());
            }
            if(i==6){
                assertEquals("strand", in.getmKey());
            }
            if(i==7){
                assertEquals("vertexCount", in.getmKey());
            }
            if(i==8){
                assertEquals("vTypes", in.getmKey());
            }
            if(i==9){
                assertEquals("vPositions", in.getmKey());
            }
            if(i==10){
                assertEquals("edgeCount", in.getmKey());
            }
            if(i==11){
                assertEquals("edgeStarts", in.getmKey());
            }
            if(i==12){
                assertEquals("edgeEnds", in.getmKey());
            }
            if(i==13){
                assertEquals("evidence", in.getmKey());
            }
            if(i==14){
                assertEquals("edgeTypes", in.getmKey());
            }
            if(i==15){
                assertEquals("mrnaRefCount", in.getmKey());
            }
            if(i==16){
                assertEquals("mrnaRefs", in.getmKey());
            }
            if(i==17){
                assertEquals("mrnaTissues", in.getmKey());
            }
            if(i==18){
                assertEquals("mrnaLibs", in.getmKey());
            }
            if(i==19){
                assertEquals("edgeEnds", in.getmKey());
            }

            
        }
    }
}
