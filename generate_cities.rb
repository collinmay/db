require "mysql2"
require "pry"

ROW_FORMAT = "q>l>Z35l>Z3l>Z30q>"

class BTree
  def initialize(f, key_size)
    @f = f
    @node_count = 0
    @key_size = key_size
    @minimum_degree = 8 # "t"
    @nodes = []
    @root = allocate_node
    update_header
  end

  # every node (except root) must have at least t - 1 keys
  #  -> at least t children
  # every node can contain at most 2t - 1 keys
  #  -> at most 2t children

  attr_reader :f
  attr_reader :root
  attr_reader :minimum_degree
  attr_reader :key_size

  def maximum_degree
    2 * @minimum_degree
  end

  def min_keys
    minimum_degree - 1
  end

  def min_children
    minimum_degree
  end
  
  def max_keys
    maximum_degree - 1
  end

  def max_children
    maximum_degree
  end
  
  def node_size
    return 4 + # number of keys
           (max_keys) * (8 + @key_size) + # keys
           (max_keys) * 8 + # values
           (max_children) * 8 + # child pointers
           1 # is leaf?
  end

  def header_size
    return 4 + # degree
           4 + # key size
           8 # root index
  end
  
  def search(key)
    @root.search(key)
  end

  def insert(key, value)
    # if root is full, split it
    if @root.is_full? then
      new_root = allocate_node
      new_root.is_leaf = false
      new_root.ptrs[0] = @root.location
      new_root.split(0, @root)
      @root = new_root
      update_header
    end
    # insert into root, whether it be new or old
    @root.insert_nonfull(key, value)
  end

  def update_header
    @f.pos = 0
    @f.write([minimum_degree, @key_size, @root.location].pack("L>L>Q>"))
  end
  
  def allocate_node
    location = next_location
    node = BTreeNode.new(self, location)
    @nodes[location] = node
    return node
  end

  def load_node(location)
    @nodes[location]||= BTreeNode.new(self, location).load
  end
  
  def next_location
    nc = @node_count
    @node_count+= 1
    return nc
  end

  def dump_tree
    @root.dump_tree(0)
  end
end

class BTreeNode
  def initialize(tree, location)
    @tree = tree
    @location = location
    @num_keys = 0
    @keys = [nil] * tree.max_keys
    @values = [0] * tree.max_keys
    @ptrs = [0] * tree.max_children
    @is_leaf = true
  end

  attr_accessor :location
  attr_accessor :num_keys
  attr_accessor :keys
  attr_accessor :ptrs
  attr_accessor :values
  attr_accessor :is_leaf

  def dump_tree(indent)
    @num_keys.times do |i|
      puts ("  " * indent) + @keys[i].unpack("Z*")[0]
      if !@is_leaf then
        @tree.load_node(@ptrs[i]).dump_tree(indent+1)
      end
    end
    puts "  " * indent + "FINAL"
    if !@is_leaf then
      @tree.load_node(@ptrs[@num_keys]).dump_tree(indent+1)
    end
  end
  
  def is_full?
    @num_keys >= @tree.max_keys
  end
  
  def load
    f = @tree.f
    f.pos = @tree.header_size + @tree.node_size * @location

    @num_keys = f.read(4).unpack("L>").first
    @keys = @tree.max_keys.times.map do
      l, b = f.read(8 + @tree.key_size).unpack("Q<a*")
      b[0, l]
    end
    @values = f.read(8 * @tree.max_keys).unpack("Q>*")
    @ptrs = f.read(8 * @tree.max_children).unpack("Q>*")
    @is_leaf = f.read(1).unpack("C").first != 0

    self
  end

  def save
    f = @tree.f
    f.pos = @tree.header_size + @tree.node_size * @location

    f.write([@num_keys].pack("L>"))
    f.write(@keys.map do |k| [(k || "").bytesize, k].pack("Q>a" + @tree.key_size.to_s) end.join)
    f.write(@values.pack("Q>*"))
    f.write(@ptrs.pack("Q>*"))
    f.write([@is_leaf ? 1 : 0].pack("C"))

    self
  end

  def search(key)
    puts "searching for #{key} (num_keys #{@num_keys})"
    i = 0
    # find i such that node.keys[i] <= key
    while(i < @num_keys && key > @keys[i])
      puts "  skipping past #{@keys[i]}"
      i+= 1
    end
    puts "  i = #{i}"
    puts "  #{@keys[i]}"
    # check if we found it
    if i < @num_keys && key == @keys[i] then
      return @values[i]
    end
    if @is_leaf then
      puts "  we are leaf"
      # if we're a leaf and the previous block didn't match, this key doesn't exist.
      return nil
    else
      puts "  checking child"
      # otherwise, check last child.
      return @tree.load_node(@ptrs[i]).search(key)
    end
  end

  def split(index, old_node)
    if @ptrs[index] != old_node.location then
      raise "node passed to split is wrong"
    end
    if is_full? then
      raise "attempt to split with full parent"
    end
    if !old_node.is_full? then
      raise "attempt to split with nonfull child"
    end
    
    new_node = @tree.allocate_node
    t = @tree.minimum_degree

    split_key = old_node.keys[t-1]
    split_value = old_node.values[t-1]
    
    old_node.num_keys = t - 1
    t.times do |j|
      new_node.keys[j] = old_node.keys[t+j]
      new_node.values[j] = old_node.values[t+j] || 0
      new_node.ptrs[j] = old_node.ptrs[t+j]
    end
    new_node.num_keys = t - 1
    
    @keys = @keys.insert(index, split_key)[0, @tree.max_keys]
    @values = @values.insert(index, split_value)[0, @tree.max_keys]
    @ptrs = @ptrs.insert(index+1, new_node.location)[0, @tree.max_children]
    @num_keys+= 1

    old_node.save
    new_node.save
    self.save
    
    [old_node, new_node]
  end

  def insert_nonfull(key, value)
    i = @num_keys - 1
    if @is_leaf then
      # shift keys over
      while(i >= 0 && key < @keys[i]) do
        @keys[i+1] = @keys[i]
        @values[i+1] = @values[i]
        @ptrs[i+1] = @ptrs[i]
        i-= 1
      end
      @keys[i+1] = key
      @values[i+1] = value
      @num_keys+= 1

      save
    else
      while(i >= 0 && key < @keys[i]) do
        i-= 1
      end
      i+= 1
      child = @tree.load_node(@ptrs[i])
      if child.is_full? then
        split(i, child)
        if key > @keys[i] then
          i+= 1
        end
      end
      @tree.load_node(@ptrs[i]).insert_nonfull(key, value)
    end
  end
end

File.open("tables/cities", "wb") do |f|
  File.open("indices/cities-names", "wb") do |btf|
    bt = BTree.new(btf, 35)
    
    # free list header row
    f.write([0, 0, "", 0, "", 0, "", 0].pack(ROW_FORMAT))

    index = 0
    client = Mysql2::Client.new(:host => "localhost", :username => "world", :database => "world")
    client.query("SELECT * FROM city").each do |row|
      fields = ["Name", "CountryCode", "District", "Population"].map do |key|
        row[key]
      end
      f.write([0, fields[0].bytesize, fields[0], fields[1].bytesize, fields[1], fields[2].bytesize, fields[2], fields[3]].pack(ROW_FORMAT))
      
      bt.insert(fields[0], index)
      index+= 1
    end

    binding.pry
  end
end
