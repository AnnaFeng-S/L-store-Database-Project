# 165A-winter-2022 (Group 14)
bufferpool的select部分。
具体的发放：BufferPool类中有bufferpool用来储存page，bufferpool_list用来储存每个bufferpool中的page的位置(第几个page_range第几个page)。laod_data方法读取文件放入内存。write_data把数据写入文件。min_use_time返回bufferpool中用的最少的page。select方法中会先检查bufferpool中有没有这个page，如果没有，那么检查bufferpool有没有满，如果没有，那就append一个page进去，如果满了，那么调用min_used_time看那个page要丢弃。查看那个page是不是dirty，如果是，则write back。

还存在的问题：(1) 现在bufferpool会在创建query的时候自动创建。根据handle，应该是在调用database中的open()时自动创建。我不太明白如何在query中调用database，因此这里可能需要修改。(2) 因为存的是page，所以如果page已经被修改可能找不到tail page？(不确定) 现在暂时用的老代码，需要修改。我想到的可能解决方法，直接在bufferpool里面存rage range (3) 现在保存文件是把一整个table保存在一个文件中，所以每次需要在disk调用时回直接把整的table放入内存中。也许分开page range储存在条用的时候可以打开特定的page range，这样不知道能不能缩短运行时间。

我后续会思考一下implement update，如果有人有时间可以改进一下这个方法。有问题可以跟我联系。

2月17号更新：
现在改成了bufferpool中装3个page range(现在直接进行page range的替换而不是page)。query中select，sum，update都implement了。并且在bufferpool类中加了pin。

现存的问题：(1) (un)pin还没有implement (2) 现在insert方法中每一次调用都会更新disk中的文件(write in)， 因此导致运行速度很慢。需要找到一个时间点当用户输入完全部数据后一次性写入数据，或者一个page range满了更新一次。(3) 现在update，sum，select中有很多重复的代码(调用bufferpool，检查bufferpool有没有满，write back之类的)，因此可能可以写一个方法直接调用。(4)bufferpool还是在创建query的时候创建，需要想办法在database创建的时候创建并且可以在query中调用。
