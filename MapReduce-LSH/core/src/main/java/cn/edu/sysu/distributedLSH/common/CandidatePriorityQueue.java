package cn.edu.sysu.distributedLSH.common;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;


public class CandidatePriorityQueue {
    private int kNeighbors = -1;

    // The candQueue is a max-heap whose capacity is kNeighbors. The implementation of
    // max-heap is PriorityQueue. It can support finding top-k minimum value efficiently.
    private Queue<Candidate> candQueue = null;


    /**
     * Default constructor.
     * */
    public CandidatePriorityQueue() {}

    /**
     * Constructor.
     * */
    public CandidatePriorityQueue(final int kNeighbors) {
        this.kNeighbors = kNeighbors;
        candQueue = new PriorityQueue<Candidate>(kNeighbors);
    }

    /**
     * size.
     * @return the size of candQueue
     * */
    public int size() {
        return candQueue.size();
    }

    /**
     * Given a collided point, update the candQueue.
     * @param point the collided point
     * @param query the query
     * @param ratioRadius  equals to ratio * currentRadius, which is cR
     * */
    public void update(final Candidate candidate) {
        if (candQueue.size() == kNeighbors) {
            // get the candidate with maximum dist in candQueue
            Candidate currentCand = candQueue.peek();
            if (candidate.getDist() < currentCand.getDist()) {
                candQueue.poll();
                // Update candidate to avoid allocating memory.
                currentCand.deepCopy(candidate);
                // insert the new candidate
                candQueue.add(currentCand);
            }
        } else {
            // insert the collided point to the candQueue
            candQueue.add(new Candidate(candidate));
        }
    }
    
    /**
     * sortedList.
     * @return the sorted list which is constructed from candQueue
     * */
    public List<Candidate> sortedList() {
        List<Candidate> list = new LinkedList<Candidate>(candQueue);

        // Sort the list by dist in ascending order.
        Collections.sort(list, new Comparator<Candidate>() {
            public int compare(Candidate a, Candidate b) {
                if (a.getDist() > b.getDist()) {
                    return 1;
                } else if (a.getDist() < b.getDist()) {
                    return -1;
                }
                return 0;
            }
        });

        return list;
    }

    /**
     * This method will convert the sorted list of the candQueue to a string.
     * */
    @Override
    public String toString() {
        List<Candidate> candList = sortedList();
        StringBuilder stringBuilder = new StringBuilder();

        for (Candidate candidate : candList) {
            stringBuilder.append(candidate.toString());
        }
        stringBuilder.append("\n");

        return stringBuilder.toString();
    }
}
