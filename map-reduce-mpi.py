# Alyssandra M. Cordero Assignment 2 Pt.1 Map Reduce
# Contains code from main template by Dr. Pruitt
import time
from mpi4py import MPI

files_to_read = ["shakespeare1.txt", "shakespeare2.txt", "shakespeare3.txt", "shakespeare4.txt",
                    "shakespeare5.txt", "shakespeare6.txt", "shakespeare7.txt", "shakespeare8.txt"]

word_list = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet", "you", "my", "blood",
             "poison", "macbeth", "king", "heart", "honest"]

# Returns a list with the words found in a document 
def read_files(file_to_read):
    with open(file_to_read, 'r') as file:
        rtn = file.read().split()
    return rtn

# Counts the number of occurrences of a word in a list of words
def get_count(word, words_list):
    count = 0
    for i in words_list:
        if word in i.lower():
            count += 1
    return count

# Returns a list of all words in all docs
def combine_docs(files_to_read):
    words = []
    for i in files_to_read:
        words.extend(read_files(i))
    return words

# Returns the dictionary with the new value
def inc_dic(past, updated):
    for key in past:
        past[key] += updated[key]#adding from value
    return past

# PARALLEL - returns the dictionary containing the count of words
def count_words_in_files(word_list, files_to_read):
    #result dictionary declaration and initialization
    result = dict.fromkeys(word_list, 0) #creates keys based on the words to find
     # get the world communicator
    comm = MPI.COMM_WORLD

    # get our rank (process #)
    rank = comm.Get_rank()

    # get the size of the communicator in # processes
    size = comm.Get_size()

    # combine all of the docs in a single list of words
    content = combine_docs(files_to_read)

    # distribute the work (by thread 0)
    if rank == 0:
        words_for_slice = len(content) / size
        # set up the local list for each s
        local_list = content[:int(words_for_slice)]
        for s in range(1, size):
            # the start and end of the slices to send
            start_of_slice = int(words_for_slice * s)
            end_of_slice = int(words_for_slice * (s + 1))
            send = content[start_of_slice:end_of_slice]
            comm.send(send, dest = s, tag = 0)
            # send message to the rest of the threads
        for w in word_list:
            result[w] = get_count(w, local_list)
        for s in range(1, size):
            temp_dic = comm.recv(source = s, tag = 1)
            result = inc_dic(result, temp_dic)
        return result
    # receive from main
    else:
        local_list = comm.recv(source = 0, tag = 0)
        for w in word_list:
            result[w] = get_count(w, local_list)
            comm.send(result, dest = 0, tag = 1)
def main():
    results = {
        'hate': 332,
        'love': 3070,
        'death': 1016,
        'night': 1402,
        'sleep': 470,
        'time': 1806,
        'henry': 661,
        'hamlet': 475,
        'you': 23304,
        'my': 14203,
        'blood': 1009,
        'poison': 139,
        'macbeth': 288,
        'king': 4545,
        'heart': 1458,
        'honest': 434
    }

    #print requirements
    start_time = time.time()
    map_reduce = count_words_in_files(word_list, files_to_read)
    end_time = time.time()
    total_time = end_time - start_time
    print('Time of Operation for Total Instances of All Words: ', total_time)
    
    for word in map_reduce:
        print(f'Total of occurrences for {word} : {map_reduce[word]}')
    print('End of the program!')

if __name__ == "__main__":
    main()