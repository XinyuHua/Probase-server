import java.io.*;
import java.net.*;
import java.util.*;

/*
 * Author: Xinyu Hua
 * Last modified: 2016-05-24
 */

public class ProbaseServerThread extends Thread {
	Socket connectionSocket;
    private ServerSocket welcomeSocket;
    private Thread t;
	private  static HashMap<String, Integer> entityIdMap;
	private  static List<String> entityList;
	private  static HashMap<Integer, List<Integer>> conceptInstanceMap;
	private  static HashMap<Integer, List<Integer>> instanceConceptMap;
    
    // This two maps below stores the number of hyponym or hypernym to speed up processing 
    private  static HashMap<Integer, Integer> conceptHypoNumMap;
    private  static HashMap<Integer, Integer> instanceHyperNumMap;

	private  static HashMap<List<Integer>, Integer[]> PairFreqMap;
    private  static HashMap<Integer, Double> conceptVaguenessMap;

	private BufferedReader commandReader;
	private PrintStream output; 
	private int pn;
	public ProbaseServerThread(int pn, ServerSocket ss, List<String> entityList, HashMap<String, Integer> entityIdMap,
		HashMap<Integer, List<Integer>> conceptInstanceMap,
		HashMap<Integer, List<Integer>> instanceConceptMap,
		HashMap<List<Integer>, Integer[]> PairFreqMap,
        HashMap<Integer, Integer> conceptHypoNumMap, HashMap<Integer, Integer> instanceHyperNumMap,
        HashMap<Integer, Double> conceptVaguenessMap){
        this.welcomeSocket = ss;
		this.pn = pn;
        this.entityIdMap = entityIdMap;
        this.entityList = entityList;
		this.conceptInstanceMap = conceptInstanceMap;
		this.instanceConceptMap = instanceConceptMap;
		this.PairFreqMap = PairFreqMap;
        this.conceptHypoNumMap = conceptHypoNumMap;
        this.instanceHyperNumMap = instanceHyperNumMap;
        this.conceptVaguenessMap = conceptVaguenessMap;

	}

    public void start(){
        t = new Thread(this);
        t.start();
    }
	
	public void run(){
        String fun = "";
        String para = "";
        boolean alive = false;
        try{
            while(true){
                connectionSocket = welcomeSocket.accept();
                alive = true;
                System.out.println("Connection Established");
                System.out.println("IP of Client:" + connectionSocket.getRemoteSocketAddress().toString());
                commandReader= new BufferedReader( new InputStreamReader( connectionSocket.getInputStream() ) );
                output = new PrintStream( connectionSocket.getOutputStream() );
                // fun to specify function name, could be 1: isProbaseEntity 2: isGoodConcept 3: isPair 4: findHypo 
                // 5: findHyper 6: getFreq 7:Bye
                System.out.println("------------------------");
                while(alive){
                    try{
                        fun = commandReader.readLine().trim();
                        if(fun.equals("isProbaseEntity")){
                            System.out.println("isProbaseEntity function called.");
                            para = commandReader.readLine().trim();
                            System.out.println( para );
                            output.println( isProbaseEntityServe( para ) );
                        }else if(fun.equals("isGoodConcept")){
                            System.out.println("isGoodConcept function called.");
                            para = commandReader.readLine().trim();
                            System.out.println( para );
                            output.println( isGoodConceptServe( para ) );
                        }else if(fun.equals("isPair")){
                            System.out.println("isPair function called.");
                            para = commandReader.readLine().trim();
                            System.out.println( para );
                            output.println( isPairServe( para ) );
                        }else if(fun.equals("findHypo")){
                            System.out.println("findHypo function called.");
                            para = commandReader.readLine().trim();
                            System.out.println( para );
                            output.println( findHypoServe( para ) );
                        }else if(fun.equals("findHyper")){
                            System.out.println("findHyper function called.");
                            para = commandReader.readLine().trim();
                            System.out.println( para );
                            String resultS = findHyperServe(para);
                            output.println( resultS);
                        }else if(fun.equals("getFreq")){
                            System.out.println("getFreq function called.");
                            para = commandReader.readLine().trim();
                            System.out.println( para );
                            output.println( getFreq(para) );
                        }else if(fun.equals("getHypoNumber")){
                            System.out.println("getHypoNumber function called.");
                            para = commandReader.readLine().trim();
                            System.out.println(para);
                            String resultS = getHypoNumberServe(para);
                            output.println( resultS );
                        }else if(fun.equals("getHyperNumber" )){
                            System.out.println("getHyperNumber function called.");
                            para = commandReader.readLine().trim();
                            System.out.println(para);
                            String resultS = getHyperNumberServe(para);
                            output.println( resultS );
                        }else if(fun.equals("getVagueness")){
                            System.out.println("getVagueness function called.");
                            para = commandReader.readLine().trim();
                            System.out.println(para);
                            String resultS = getVaguenessServe(para);
                            output.println( resultS );
                            System.out.println(resultS);
                        }else if(fun.equals("bye")){
                            System.out.println("exit function called.");
                            alive = false;
                        }
                    }
                    catch(Exception e){
                            System.out.println("server:" + pn +", Client disconnect with " + e);
                            e.printStackTrace();
                            break;
                    }
                }
                System.out.println("Client disconnected");
            }
        }catch(Exception e){
			e.printStackTrace();
		}
        
    }

    public String getFreq(String para) throws Exception
    {   
        if( para.trim().equals("_") ){
            System.out.println("Query empty concept and empty instance, return 100");
            return "100_100";
        }

        String[] splitted = para.split("_");
        if( splitted.length != 2 ){ 
            System.out.println("One of the query is empty, return 0");
            return "0_0";
        }

        Integer[] result = getFreq(para.split("_")[0], para.split("_")[1] );
        
        return Integer.toString( result[ 0 ] ) + "_" + Integer.toString( result[ 1 ] );
    }

    public String isProbaseEntityServe(String obj)throws Exception
    {
        if( isProbaseEntity( obj ) )
            return "true";
        return "false";
    }

    public String isPairServe(String para)throws Exception
    {
        String[] splitted = para.split("_");
		if( isPair( splitted[0] , splitted[1] )){
            System.out.println(para + ": true");
            return "true";
        }
        return "false";
    }

    public String isGoodConceptServe(String obj)throws Exception
    {
        if( isGoodConcept( obj ) )
            return "true";
        return "false";
    }

    public String findHypoServe(String concept)throws Exception
    {
        List<String> rstList = findHypo( concept );
        String result = "";
        if(! rstList.isEmpty() )
        {
            for( String st : rstList)
            {
                result += st + "_";
            }
        }

        return result;
    }

    public String findHyperServe(String instance)throws Exception
    {
        List<String> rstList = findHyper( instance );
        
        String result = "";
        if(! rstList.isEmpty() ){
            for( String st : rstList){
                result += st + "_";
            }
        }

        return result;

    }

    public String getHypoNumberServe(String concept)throws Exception{
        int hypoNumber = getHypoNumber(concept);
        return Integer.toString(hypoNumber);
    }

    public String getHyperNumberServe(String instance)throws Exception{
        int hyperNumber = getHyperNumber(instance);
        return Integer.toString(hyperNumber);
    }

    public String getVaguenessServe(String concept)throws Exception{
        Double vagueness = getVagueness( concept );
        return Double.toString(vagueness);
    }

	private boolean isProbaseEntity(String obj)throws Exception
	{
        obj = obj.trim();
        if( entityIdMap.containsKey( obj ) )
            return true;
        return false;
	}
	
	private boolean isPair(String concept, String instance)throws Exception
	{
        if(concept.trim().equals("") || instance.trim().equals(""))return false;

        if(!entityIdMap.containsKey(concept) || !entityIdMap.containsKey(instance)) 
            return false;
        if(concept.equals(instance))return true;
        Integer conceptId = entityIdMap.get(concept);
        Integer instanceId = entityIdMap.get(instance); 
        if(!conceptInstanceMap.containsKey(conceptId)){
            return false;
        }
        List<Integer> instanceList = conceptInstanceMap.get(conceptId);
        if( instanceList.contains(instanceId))
            return true;

        return false;
    }

	private boolean isGoodConcept(String concept)throws Exception
	{
		if(entityIdMap.containsKey(concept))
		{
			Integer cId = entityIdMap.get(concept);
			List<Integer> tmpList = conceptInstanceMap.get(cId);
			if(tmpList.size() > 10)
            {
                return true;
		
            }
        }
		return false;
	}
    
    private Integer[] getFreq(String concept, String instance) throws Exception
    {   
        concept = concept.trim();
        instance = instance.trim();
        Integer[] defaultAns = {0,0};

        if(concept.equals(instance) && entityIdMap.containsKey(instance)){
            defaultAns[0] = 100;
            defaultAns[1] = 100; 
        }
        else if( isPair( concept, instance ) )
        {
            Integer[] pair = { entityIdMap.get(concept), entityIdMap.get(instance) };
            return PairFreqMap.get( Arrays.asList( pair ) );
        }
        return defaultAns;
    }

    private List<String> findHypo(String concept) throws Exception
    {
        List<String> hypoList = new ArrayList<String>();
        if(entityIdMap.containsKey(concept))
        {
            Integer cId = entityIdMap.get(concept);
            if(conceptInstanceMap.containsKey(cId)){
                List<Integer> tmpList = conceptInstanceMap.get(cId);
                for(Integer iId : tmpList)
                {
                    hypoList.add(entityList.get(iId));
                }
            }
        }
        return hypoList;
    }

	private List<String> findHyper(String instance) throws Exception
	{
		List<String> hyperList = new ArrayList<String>();
        if(entityIdMap.containsKey(instance)){
		    HashMap<String, Integer> hyper2Freq = new HashMap<String, Integer>();
			Integer iId = entityIdMap.get(instance);
            if( instanceConceptMap.containsKey( iId ) ) {
			    List<Integer> tmpList = instanceConceptMap.get(iId);
                for(Integer cId : tmpList){
                        String concept = entityList.get(cId);
                        Integer[] pair= {cId, iId};
                        hyper2Freq.put(concept, PairFreqMap.get(Arrays.asList(pair))[0]);
                }
                ValueComparator bvc = new ValueComparator(hyper2Freq);
                TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(bvc);
                sortedMap.putAll(hyper2Freq);

                for(String concept : sortedMap.keySet()){
                    hyperList.add(concept);
                }
            }
        }
		return hyperList;
	}

    private int getHypoNumber(String concept)throws Exception{
        int result = 0;
        /*
        if(ConceptIdMap.containsKey(concept)){
            Integer cId = ConceptIdMap.get(concept);
            return ConceptHypoNumMap.get(cId);
        }*/
        if(entityIdMap.containsKey(concept)){
            Integer cId = entityIdMap.get(concept);
            if( conceptInstanceMap.containsKey(cId) ) {
                result = conceptInstanceMap.get(cId).size();
            } 
        }
        return result;
    }

    private int getHyperNumber(String instance)throws Exception{
        int result = 0;
        /*
        if(InstanceIdMap.containsKey(instance)){
            Integer iId = InstanceIdMap.get(instance);
            return InstanceHyperNumMap.get(iId);
        }
        */
        if(entityIdMap.containsKey(instance)){
            Integer iId = entityIdMap.get(instance);
            if( instanceConceptMap.containsKey( iId )) {
                result = instanceConceptMap.get(iId).size();
            }
        }
        return result;
    }

    private Double getVagueness(String concept)throws Exception{
        Double result = 0.0;
        if( entityIdMap.containsKey( concept ) ){
            Integer cId = entityIdMap.get( concept );
            if( conceptVaguenessMap.containsKey( cId ) ){
                result = conceptVaguenessMap.get( cId );
            }
        }
        return result;
    }
}

class ValueComparator implements Comparator<String>
{
	Map<String, Integer> base;
	public ValueComparator( Map<String, Integer> base)
	{
		this.base = base;
	}
	
	public int compare(String a, String b)
	{
		if(base.get(a) >= base.get(b))
			return -1;
		else 
			return 1;
	}
}
