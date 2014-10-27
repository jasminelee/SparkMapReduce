from pyspark import SparkContext
import Sliding, argparse

#expand the board position to its children using sliding.children(h, w, position)
#rdd is how Spark represents the list of items to call map on or to reduce, etc. it's not indexable or iterable. 

# apply a func to every single item in RDD
# use set to check if a node's aready been visited
# bfs map only sees a tuple at a time. the 
# find possible moves within bfs_map using board from prev level
# new rdd = exclusive moves within current level. use for next iteration, level++

# RDD.flatMap(bfs_map()) repeatedly do this. 
#return list of kv pairs 
def bfs_map(value):
    """ YOUR CODE HERE """
<<<<<<< HEAD
    level +=1
#    value[1] += 1 #
=======

    level += 1 #
>>>>>>> 63ffd4def32b1e57dc06bb66fe333013ac4ccdea
       # get children, make tuples, make a list
    resultList = []
    result.append(value) #ensure parent is linked with children below
    result.append(map(lambda x: (x,level), Sliding.children(WIDTH, HEIGHT, value[0]) ))#get first item of board
#        RDD.flatMap(lambda x: (x, level))
    return result # for bfs_reduce
  
# do an operation on every single item, two at a time
# want reduce to go back into map s
# remove copies of moves here/filter 

# reduceByKey returns many similar boards, use write bfs_reduce to get minimum key, 

def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    # reduceByKey reduces matching tuples together. use min? 
    return min(value1, value2)
    #RDD.reduceByKey(lambda a, b: a[0] == b[0], (value1[0] value2[0])) #if keys are equal remove

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT=height
    WIDTH=width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)


    """ YOUR MAP REDUCE PROCESSING CODE HERE """
    sol = (sol, 0) #create initial tuple of solution board, level)
    RDD = sc.parallelize(bfs_map(sol))

    #while RDD hasn't changed/ while level hasn't changed? /size of tree is no longer changing?/ RDD.count()

    oldSize, newSize = RDD.count(), RDD.count()

    while 1: 
        if newSize == oldSize:
            break
        else:
#            level += 1
            oldSize = RDD.count() 
            RDD.flatMap(bfs_map).reduceByKey(bfs_reduce)
            newSize = RDD.count()

    RDD.collect() #lazyeval

    #RDD.bfs_map(sol).bfs_reduce().collect()#
    #call partition then collect.()? collect is serial/no parallel so be careful 


    #base case: when there are no more boards at a level 
    #global vars, reducing by keys -- 


    """ YOUR OUTPUT CODE HERE """

    sc.stop()



""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
