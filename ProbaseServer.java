import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Author: Xinyu Hua
 * Last modified: 2016-6-22
 * Functions: 1) Pending server, waiting for client, no need to load data multiple times
 *            2) Multiple server concurrency
 *            3) Elegantly quit
 *            4) Speeded up hypernum and hyponum size finder
 * Note: Don't use comma as separator! Use _ or something not in the dict
 */

public class ProbaseServer {
	
	static final String ENTITY_DICT_URL =  "entity_dict_core.txt";
	static final String PAIR_URL = "matrix_core.txt";
	
    private ServerSocket welcomeSocket;
	private  static HashMap<String, Integer> entityIdMap;
	private  static List<String> entityList;
	private  static HashSet<String> entitySet; 
	private  static HashMap<Integer, List<Integer>> conceptInstanceMap;
	private  static HashMap<Integer, List<Integer>> instanceConceptMap;
	private  static HashMap<List<Integer>, Integer[]> pairFreqMap;
    
    private  static HashMap<Integer, Integer> conceptHypoNumMap;
    private  static HashMap<Integer, Integer> instanceHyperNumMap;
    private  static HashMap<Integer, Double> conceptVaguenessMap;
    private int portNum; 
	
    public static void main(String[] args)throws Exception{
    	loadProbase();
		int socketNum = Integer.parseInt(args[1]);
		int base = Integer.parseInt(args[0]);
		ProbaseServer[] psArray = new ProbaseServer[ socketNum ];
		for(int i = 0; i < socketNum; ++i){
           // System.out.println("Should start:" + Integer.toString( base + i));
			psArray[ i ] = new ProbaseServer(base + i );
			psArray[ i ].runServer();
		}
    }
    
	public ProbaseServer(int PORT_NUM) throws Exception{
       // System.out.println("Starting server..." + PORT_NUM);
        portNum = PORT_NUM;
        welcomeSocket = new ServerSocket( PORT_NUM );
    }
	
    public void runServer()throws Exception{
    	    new ProbaseServerThread(portNum, welcomeSocket, entityList, entityIdMap, 
    				conceptInstanceMap,instanceConceptMap,pairFreqMap,  
                    conceptHypoNumMap, instanceHyperNumMap, conceptVaguenessMap).start();
    }
	
	private static void loadProbase() throws Exception
	{
        File pairFile = new File( PAIR_URL );
		File entityFile = new File(ENTITY_DICT_URL);
		BufferedReader entityReader = new BufferedReader(new FileReader(entityFile));
		BufferedReader pairReader = new BufferedReader(new FileReader(pairFile));
		String line = null;

        entityIdMap = new HashMap<>();
        entityList = new ArrayList<>();
        conceptInstanceMap = new HashMap<>();
        instanceConceptMap = new HashMap<>();
        pairFreqMap = new HashMap<>();
        conceptHypoNumMap = new HashMap<>();
        instanceHyperNumMap = new HashMap<>();
        conceptVaguenessMap = new HashMap<>();
	    
		System.out.println("Loading Probase ...(will take around 3 minutes)");

		long startTime = System.currentTimeMillis();
		System.out.println("--Loading concept dictionary...");
		while((line = entityReader.readLine()) != null)
		{
		    String [] splitted = line.split("\t");
            String entity = splitted[1];//.toLowerCase();
			//String [] splitted = line.split("\\s+");
			Integer id = Integer.parseInt(splitted[0]);
            entityIdMap.put( entity, id );
			entityList.add(entity);
		}
		entityReader.close();
		System.gc();
		
		int cnt = 0;
		System.out.println("--Loading relation matrix...");
		while((line = pairReader.readLine()) != null)
		{
			String [] splitted = line.split("\t");
			//String [] splitted = line.split("\\s+");
			Integer conceptId = Integer.parseInt(splitted[0]), instanceId = Integer.parseInt(splitted[1]);
            Integer freq = Integer.parseInt(splitted[2]), popularity = Integer.parseInt(splitted[3]);
            Integer conceptSize = Integer.parseInt(splitted[4]);
            Double conceptVagueness = -1.0;
            if(!splitted[5].equals("NULL")){
                conceptVagueness = Double.parseDouble(splitted[5]);
            }

			if(!conceptInstanceMap.containsKey(conceptId)){
                conceptInstanceMap.put( conceptId, new ArrayList<>() );
            }

            conceptInstanceMap.get( conceptId ).add( instanceId );
			
		    if(!instanceConceptMap.containsKey( instanceId ) ){
                instanceConceptMap.put( instanceId, new ArrayList<>() );
            }

            instanceConceptMap.get( instanceId ).add( conceptId );
	
            if(!conceptHypoNumMap.containsKey(conceptId)){
                conceptHypoNumMap.put(conceptId, conceptSize );
            }

            if(!conceptVaguenessMap.containsKey( conceptId ) ){
                conceptVaguenessMap.put( conceptId, conceptVagueness );
            }
            
            if(instanceHyperNumMap.containsKey(instanceId)){
                instanceHyperNumMap.put(instanceId, instanceHyperNumMap.get(instanceId) + 1 );
            }else{
                instanceHyperNumMap.put(instanceId, 1 );
            }

			Integer [] pair = { conceptId, instanceId };
            Integer [] freqPair = { freq, popularity };
			pairFreqMap.put(Arrays.asList(pair), freqPair);
			
			++cnt;
			if(cnt % 1000000 == 0)
			{
                System.out.println( Integer.toString( cnt ) + " Lines loaded(13949064 lines in total )");
				System.gc();
			}
		}
		pairReader.close();
		System.gc();
		
		
		System.out.println("Probase core52 loaded:)");
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    System.out.println("Time elapsed:"+Long.toString(elapsedTime/1000) + "sec");
	}
	
}

