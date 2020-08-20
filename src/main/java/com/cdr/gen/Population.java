package com.cdr.gen;

import com.cdr.gen.util.RandomUtil;
import com.cdr.gen.util.RandomGaussian;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;

/**
 * This class creates the defined number of customers and a set of calls following
 * the gaussian distribution.
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class Population {
    private static final Logger LOG = Logger.getLogger(Population.class);
    private int size;
    private int fraudCount;
    private int fraudDistance;
    private boolean fraudForceAll;
    private Map<String, Long> callsMade;
    private Map<String, Long> phoneLines;
    private List<String> callTypes;
    private Map<String, Object> outgoingCallParams;
    private List<Person> population;

    private CellDistribution cellDist;
    private CallDistribution callDist;
    private DateTimeDistribution dateTimeDist;

    private PhoneBucketGenerator phoneBucketGen;
    private Map<String, Cell> lastPhoneNumberCell;
    
    private Random random;
    
    public Population(Map<String, Object> config) {
        this.size  = ((Long)config.get("numAccounts")).intValue();
        Map<String, Object> fraud = (Map<String, Object>)config.get("fraud");
        this.fraudCount = ((Long) fraud.get("count")).intValue();
        this.fraudDistance = ((Long) fraud.get("distance")).intValue();
        this.fraudForceAll = "true".equals(fraud.get("forceAll").toString());
        callsMade  = (Map<String, Long>) config.get("callsMade");
        phoneLines = (Map<String, Long>) config.get("phoneLines");
        callTypes  = (List<String>) config.get("callTypes");
        outgoingCallParams = (Map<String, Object>) config.get("outgoingCallParams");
        population = new ArrayList<Person>(size);

        lastPhoneNumberCell = new HashMap<>();

        cellDist = new CellDistribution();
        callDist = new CallDistribution(config);
        dateTimeDist = new DateTimeDistribution(config);
        
        phoneBucketGen = new PhoneBucketGenerator(config);
        
        random = new Random(System.currentTimeMillis());
    }
    
    /**
     * Create the population
     */
    public void create() {
        RandomGaussian gaussNum;

        for (int i=0; i<size; i+=2) {
            LOG.debug("Creating person " + (i+1) + " and " + (i+2));
            Person personOne = new Person();
            Person personTwo = new Person();
            
            // create the phone number
            LOG.debug("Generating phone numbers");
            personOne.setPhoneNumber(getRandomPhoneNumber());
            personTwo.setPhoneNumber(getRandomPhoneNumber());
            
            // calculate the number of calls made
            LOG.debug("Calculating number of calls made");
            gaussNum = getRandomGaussian(callsMade.get("stdDev"), callsMade.get("mean"));

            personOne.setNumCalls(gaussNum.getValueOne().longValue());
            personTwo.setNumCalls(gaussNum.getValueTwo().longValue());
            
            // calculate the average duration of a call per type
            LOG.debug("Calculating the average duration of a call per type");
            for (String callType : callTypes) {
                // peak time
                gaussNum = getAvgCallDuration(callType, false);
                personOne.getAvgCallDuration().put(callType, gaussNum.getValueOne().longValue());
                personTwo.getAvgCallDuration().put(callType, gaussNum.getValueTwo().longValue());
                
                // off peak
                gaussNum = getAvgCallDuration(callType, true);
                personOne.getAvgOffPeakCallDuration().put(callType, gaussNum.getValueOne().longValue());
                personTwo.getAvgOffPeakCallDuration().put(callType, gaussNum.getValueTwo().longValue());
            }
            
            // generate the number of phone lines
            LOG.debug("Generating the number of phone lines");
            if (phoneLines.get("mean") > 1) {
                if (phoneLines.get("stdDev") > 1) {
                    gaussNum = getRandomGaussian(phoneLines.get("stdDev"), phoneLines.get("mean"));
                    personOne.setPhoneLines(gaussNum.getValueOne().intValue());
                    personTwo.setPhoneLines(gaussNum.getValueTwo().intValue());
                } else {
                    personOne.setPhoneLines(((Long)phoneLines.get("mean")).intValue());
                    personTwo.setPhoneLines(((Long)phoneLines.get("mean")).intValue());
                }
            } else {
                personOne.setPhoneLines(1);
                personTwo.setPhoneLines(1);
            }
            
            // create the user calls
            LOG.debug("Creating the calls for person " + (i+1));
            createCalls(personOne);
            
            LOG.debug("Creating the calls for person " + (i+2));
            createCalls(personTwo);

            population.add(personOne);
            population.add(personTwo);
        }

        Stream<Call> callStream = population.stream()
                .flatMap(p -> p.getCalls().stream());

        if (fraudForceAll) {
            callStream = callStream.peek(c -> c.setFraud(Fraud.UNUSUAL));
        }

        if (fraudCount > 0) {

            List<Call> allCalls = callStream
                    .collect(Collectors.toList());

            // Create fraud calls
            Random random = new Random();
            List<Call> fraudCalls = random.ints(fraudCount, 0, allCalls.size())
                    .mapToObj(allCalls::get)
                    .map(Call::copy)
                    .map(this::toFraudCall)
                    .collect(Collectors.toList());

            // Inject fraud calls
            fraudCalls.forEach(c -> {
                population.stream()
                        .filter(p -> p.getCalls().contains(c))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("No call found"))
                        .getCalls().add(c.copyWithId(UUID.randomUUID()));
            });
        }
    }

    private Call toFraudCall(Call call) {
        int diffStartTime = RandomUtil.randInt(5, 1500);
        int callDurationInSec = RandomUtil.randInt(1, 600);

        DateTime startCall = call.getTime().getStart().plusSeconds(diffStartTime);
        DateTime endCall = startCall.plusSeconds(callDurationInSec);
        call.setTime(new Interval(startCall, endCall));

        double distanceInMeters = fraudDistance * 1000;
        Cell originalCell = call.getCell();
        Cell otherCell = cellDist.getRandomCell(originalCell.getId(), distanceInMeters);
        call.setType(callDist.getRandomCallType());
        call.setCell(otherCell);
        call.setDestPhoneNumber(createNewPhoneNumber(call.getDestPhoneNumber()));
        call.setCost(dateTimeDist.getCallCost(call));
        call.setFraud(Fraud.FAR);
        return call;
    }

    private String createNewPhoneNumber(String phoneNumber) {
        int phoneEnd = RandomUtil.randInt(1, 9999);
        return String.format("%s%04d", phoneNumber.substring(0, phoneNumber.length() - 4), phoneEnd);
    }
    
    /**
     * Generates random gaussian numbers until they become greater than one.
     * @param stdDev The standard deviation
     * @param mean The average
     * @return A set of two random numbers
     */
    protected RandomGaussian getRandomGaussian(long stdDev, long mean) {
        RandomGaussian gaussNum;
        
        do {
            gaussNum = RandomGaussian.generate(stdDev, mean);
        } while (gaussNum.getValueOne() < 1 || gaussNum.getValueTwo() < 1);
        
        return gaussNum;
    }
    
    /**
     * Calculates a random average for the call duration of a given type.
     * @param callType The type of call for which the average will be calculated
     * @param offPeak A boolean informing if the average is for off peak or not
     * @return A set of two random numbers
     */
    protected RandomGaussian getAvgCallDuration(String callType, boolean offPeak) {
        Map<String, Object> conf = (Map<String, Object>) outgoingCallParams.get(callType);
        
        String meanKey = (offPeak) ? "callOPDur" : "callDur";
        String stdDevKey = (offPeak) ? "callOPStdDev" : "callStdDev";
        
        return getRandomGaussian((Long)conf.get(stdDevKey), (Long)conf.get(meanKey));
    }

    /**
     * Generates a random phone number of 11 digits.
     * @return The randomly generated phone number
     */
    protected String getRandomPhoneNumber() {
        String code = PhoneNumberGenerator.getRandomPhoneCode("Local", "");
        return code + PhoneNumberGenerator.getRandomNumber(11 - code.length());
    }
    
    /**
     * Create all calls for a given person according to the number of calls calculated
     * for the person.
     * @param p The person for which the calls will be made
     */
    protected void createCalls(Person p) {
        Map<String, List<Interval>> usedTimes = new HashMap<String, List<Interval>>();

        // create a list of call types for each call made
        // it is created beforehand so that we can generate the phone bucket
        String[] listOfCallTypes = new String[(int) p.getNumCalls()];
        Map<String, Integer> callTypeSummary = new HashMap<String, Integer>();
        
        for (int i=0; i<p.getNumCalls(); i++) {
            String callType = callDist.getRandomCallType();
            
            if (callTypeSummary.containsKey(callType)) {
                callTypeSummary.put(callType, callTypeSummary.get(callType)+1);
            } else {
                callTypeSummary.put(callType, 1);
            }
            
            listOfCallTypes[i] = callType;
        }
        
        Map<String, List<String>> phoneBucket = phoneBucketGen.createPhoneBucket(p, callTypeSummary);

        Cell lastCell = getLastPhoneNumberCell(p.getPhoneNumber());

        for (int i=0; i<p.getNumCalls(); i++) {
            Call call = new Call();
            call.setId(UUID.randomUUID());
            call.setCell(lastCell);
            call.setType(listOfCallTypes[i]);
            call.setLine((int) (random.nextDouble() * p.getPhoneLines() + 0.5));
            
            // pick a random destination phone number
            call.setDestPhoneNumber(phoneBucket.get(listOfCallTypes[i]).get(
                    RandomUtil.randInt(0, phoneBucket.get(listOfCallTypes[i]).size()-1)));
            
            
            long avgCallDuration = p.getAvgCallDuration().get(call.getType());
            long avgOPCallDuration = p.getAvgOffPeakCallDuration().get(call.getType());

            // pick a random date that doesn't overlap any other calls
            do {
                int currDay = dateTimeDist.getDayOfWeek();
                int currDayName = dateTimeDist.getStartDate().plusDays(currDay).getDayOfWeek();
                String type = (currDayName == 1 || currDayName == 7) 
                        ? DateTimeDistribution.TYPE_WEEKEND 
                        : DateTimeDistribution.TYPE_WEEKDAY;
                
                DateTime dateTime = dateTimeDist.getDateTime(type, currDay);
                int duration = dateTimeDist.getCallDuration(currDayName, 
                        call.getType(), dateTime.toLocalTime(), avgCallDuration, 
                        avgOPCallDuration);
                
                call.setTime(new Interval(dateTime, dateTime.plusMinutes(duration)));
            } while (callIntervalOverlap(usedTimes, call.getTime()));
        
            // after the date has been picked, calculate the cost of the call
            call.setCost(dateTimeDist.getCallCost(call));
            
            p.getCalls().add(call);
        }
    }

    private Cell getLastPhoneNumberCell(String phoneNumber) {
        Cell cell = lastPhoneNumberCell.get(phoneNumber);
        if (cell == null) {
            cell = cellDist.getRandomCell();
            lastPhoneNumberCell.put(phoneNumber, cell);
        }
        return cell;
    }

    /**
     * Checks if a call time interval is already in use. This function is used to
     * prevent to calls from the same person happening at the same time.
     * @param usedTimes A collection which stores already consolidated time intervals
     * @param time The time interval to be checked
     * @return True if the time interval already exists or False otherwise
     */
    protected boolean callIntervalOverlap(Map<String, List<Interval>> usedTimes, Interval time) {
        String date = time.getStart().toString(DateTimeFormat.forPattern("dd/MM/yyyy"));
        
        if (usedTimes.containsKey(date)) {
            for (Interval i : usedTimes.get(date)) {
                if (time.overlap(i) != null)
                    return true;
            }
        } else {
            usedTimes.put(date, new ArrayList<Interval>());
        }
        
        usedTimes.get(date).add(time);
        
        return false;
    }

    /**
     * @return The generated population
     */
    public List<Person> getPopulation() {
        return population;
    }
    
    
}
