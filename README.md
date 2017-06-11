This summarizes the design and implementation.

# Layered Approach
Concurrency complicates things, and mixing it in directly with core logic muddles up the code. Effort is taken therefore to adopt a layered approach, where the data structure and algorithm is designed without the concerns on concurrency, and leaves that to a layer on top of the core.

# Data Structure and Algorithm
One dedicated internal FIFO queue exists for each priority: Q<sub>P</sub> for items of priority P. An enqueue of priority P always goes into its dedicated queue Q<sub>P</sub>. To observe Constraint A, dequeue comes from the nonempty queue of the highest priority existing, unless there is a burst (see below). 

If there is a burst rate defined for a priority, the count of dequeue of that priority is tracked in order to observe Constraint B. If the count reaches the burst rate, the next dequeue first checks the highest available priority below the burst one; if no item exists there, it falls back to observe Constraint A. 

This tackles __Problem A1__. With the original design, if there is no more priority P+1 (either existing or new inbound items) after the burst of priority P, consumers would be blocked forever even if there is still items of other priorities. In that case not only priority P+2, P+3, ... would not be consumed, higher priorities like 1, 2, ,,,, P would not either. If the blocking continues until the queue is full, producers would be blocked too, then the whole queue is dead.
  
## Limitation
  The approach is efficient when the number of possible priorities is a relatively small number, as a too large value of this number would make too many internal queues be created.
  
# Implementation and Technical Stack

1. Plain Scala: for core data structure and algorithm

* Generalization of Burst Rate (__Problem B__)
 
    A partial function is used to model the burst rate per priority. _Only if_ the function is defined at P, then priority P has a burst rate, and burst for that priority is tracked. So to define the original "all two" burst rate:
    ``` 
        val burstRate = {
            case _ => 2
        }
    ```
    This is also the default if not explicitly specified.
    
    Another example, say, priorities of 1 - 3 have a rate of 5, 4 - 8 a rate of 10, no burst rate (hence no burst track) for the rest:
    ```
        val burstRate = {
            case i if 1 to 3 contains i => 5
            case i if 4 to 8 contains i => 10
        }    
    ```
   
2. Akka Actor

    Actor model is used for efficient, and scalable concurrency. 
    
# Build, and Test
    
## Prerequisites

1. Java 8
2. sbt (tested to work with v.0.13.15)
2. Internet connection (to download dependencies)

## Run

1. clone code

    ```git clone https://github.com/happyschemer/q.git```    
    
2. run test cases
    
    ```sbt test```