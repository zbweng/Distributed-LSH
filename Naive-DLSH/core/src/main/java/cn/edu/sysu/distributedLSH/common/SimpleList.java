package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * SimpleList is implemented as a very simple singly linked list.
 * */
public class SimpleList implements Writable {
    /**
     * The node for SimpleList, or the list. We only consider storing int.
     * */
    private static class Node {
        int e;         // the element in the SimpleList, e >= 0
        Node next;

        Node(final int e, final Node next) {
            this.e = e;
            this.next = next;
        }
    }


    private Node head;          // points to the first element in the list
    private Node cursor;        // a cursor for traversing the list
    private int listSize;       // the size of the SimpleList


    /**
     * Constructor. Create an empty list.
     * */
    public SimpleList() {
        head = new Node(-1, null);
        cursor = head;
        listSize = 0;
    }

    /**
     * Add a node to the beginning of the list.
     * */
    public void add(final int e) {
        Node node = new Node(e, null);
        node.next = head.next;
        head.next = node;
        listSize++;
    }

    /**
     * size.
     * */
    public int size() {
        return listSize;
    }

    /**
     * clear.
     * */
    public void clear() {
        head.next = null;
        cursor = head;
        listSize = 0;
    }

    /**
     * setCursorToHead.
     * Make cursor point to the head of the SimpleList.
     * */
    public void setCursorToHead() {
        cursor = head;
    }

    /**
     * hasNext.
     * */
    public boolean hasNext() {
        if (null != cursor.next) {
            return true;
        }
        return false;
    }

    /**
     * next.
     * */
    public int next() {
        cursor = cursor.next;
        return cursor.e;
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void write(final DataOutput out) throws IOException {
        out.writeInt(listSize);
        cursor = head;
        while (null != cursor.next) {
            cursor = cursor.next;
            out.writeInt(cursor.e);
        }
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void readFields(final DataInput in) throws IOException {
        // Since Writable is always reused by Hadoop, we can make use of
        // current SimpleList to avoid allocating memory if possible.
        // Attention!
        // We do not consider the order of the elements in SimpleList.
        final int size = in.readInt();

        if (size <= listSize) {
            cursor = head;
            for (int i = 0; i < size; i++) {
                cursor = cursor.next;
                cursor.e = in.readInt();
            }
            cursor.next = null;
        } else {
            // We must allocate (size - listSize) new Node.
            cursor = head;
            while (null != cursor.next) {
                cursor = cursor.next;
                cursor.e = in.readInt();
            }

            int remain = size - listSize;
            for (int i = 0; i < remain; i++) {
                Node node = new Node(in.readInt(), null);
                node.next = head.next;
                head.next = node;
            }
        }

        cursor = head;
        listSize = size;
    }
}
