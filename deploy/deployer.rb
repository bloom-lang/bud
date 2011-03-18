require 'rubygems'
require 'bud'
require 'open-uri'

module Deployer

  class GenericBud
    include Bud
    include Deployer
  end

  state do
    channel :rule_chan, [:@loc, :sender, :array]
    channel :decl_chan, [:@loc, :sender, :array]
    channel :rule_ack, [:@loc, :sender, :port]
    channel :decl_ack, [:@loc, :sender]
    table :persist_rule_ack, [:loc, :sender, :port]
    table :persist_decl_ack, [:loc, :sender]
    table :node, [:uid] => [:node] # nodes with running programs
    table :deploy_node, [:uid] => [:node] # nodes to deploy onto
    table :dead, [:dead]
    table :ack, [:node]
    scratch :not_all_in, [:bool]
    table :initial_data, [:uid, :data]
    channel :initial_data_chan, [:@node, :data]
    scratch :dont_care, [:dont_care]
  end

  def initialize opt
    super
    @new_instance = nil
    if opt[:nat]
      @my_ip = open("http://myip.dk") { |f| /([0-9]{1,3}\.){3}[0-9]{1,3}/.match(f.read)[0].to_a[0] }
    else
      @my_ip = @ip
    end
  end

  # current node
  def me() @my_ip + ":" + @port.to_s end

  # rule killswitch + side-effect safety
  def idempotent(r) (dead.include? r) ? false : dead.insert(r) end

  # tables we don't want want to distribute rules or decls for
  #
  # XXX: ensure these don't clash with user tables
  # XXX: modularize this by moving the EC2 stuff into the EC2 module
  def bootstrap_tables
    ["rule_chan", "decl_chan", "rule_ack", "decl_ack", "persist_rule_ack",
     "persist_decl_ack", "node", "dead", "ack", "not_all_in", "initial_data",
     "initial_data_chan", "dont_care", "rule", "decl", "node_decl_ack",
     "max_count", "min_count", "access_key_id", "secret_access_key", "image_id",
     "key_name", "ec2_key_location", "ec2_conn", "ec2_insts", "reservation_id",
     "init_command", "spinup_timer", "the_reservation", "the_reservation_next",
     "node_up", "node_ssh", "init_dir", "temp_node", "all_up", "deploy_node",
     "node_count"]
  end

  # eval code in the bud instance
  def safe_eval(str, lambda)
    begin
      puts "Safe evaling: " + str
      $stdout.flush
      lambda.call(str)
    rescue Exception => exc
      puts "#{$!}"
      return false
    end
    return true
  end

  # add rules to the MetaRecv class
  def insert_rules rules
    if safe_eval("bloom :recv_rules do\n" + rules.join("\n") + "\nend",
                 lambda {|s| GenericBud.class_eval(s)})
      begin
        @new_instance = GenericBud.new(:ip => "127.0.0.1")
        @new_instance.run_bg
      rescue Exception => exc
        puts "#{$!}"
        return false
      end
      return true
    else
      return false
    end
  end

  # add decls to the instance
  #
  # XXX: will need to add these to the MetaRecv class rather than the instance
  # when Peter's decl analysis actually does something; otherwise his analysis
  # won't pick these up
  def insert_decls decls
    return safe_eval("state{\n" + decls.join("\n") + "\n}",
                     lambda {|s| GenericBud.class_eval(s)})
  end

  # read the program from the metamodel and send
  #
  # XXX: won't transfer any non-rule code
  bloom :rule_send do
    rule = [@meta_parser.rules.find_all do |r|
              not bootstrap_tables.include? r[1]
            end.map {|r| r[3]}]
    decl = [@meta_parser.decls.find_all do |d|
              not bootstrap_tables.include? d[0]
            end.map {|d| d[1]}]

    # send decls before rules
    decl_chan <~ deploy_node.map do |n|
      [n.node, me, decl] if idempotent [[n.node, decl]]
    end
    node_decl_ack = join([deploy_node, decl_ack],
                         [deploy_node.node, decl_ack.sender])
    rule_chan <~ node_decl_ack.map do |n, r|
      [n.node, me, rule] if idempotent [[n.node, rule]]
    end
  end

  # reify the program, and send back an ack if the rule adding was successful
  bloom :rule_recv do
    rule_ack <~ rule_chan.map do |r|
      # XXX: hack to get around assignment problem
      [r.sender, me, @new_instance.port] if insert_rules r.array
    end
    decl_ack <~ decl_chan.map do |d|
      [d.sender, me] if insert_decls d.array
    end

    persist_rule_ack <= rule_ack
    # insert the IP and port of the node into "node"
    node <= join([rule_ack, deploy_node],
                 [rule_ack.sender, deploy_node.node]).map do |r, d|
      [d.uid, r.sender.split(':')[0] + ":" + r.port.to_s]
    end
    persist_decl_ack <= decl_ack
  end

  # check to make sure every node has received the package of rules and decls
  bloom :consensus do
    ack <= join([persist_rule_ack, persist_decl_ack],
                [persist_rule_ack.sender, persist_decl_ack.sender]).map do |r,d|
      ((puts r.sender + " has received all rules and decls") if idempotent [[:ack, r.sender]]) or [r.sender]
    end

    #i want to use this rule, but I can't
    #all_in <= [true] if node.all? {|n| ack.include? n}
    not_all_in <= deploy_node.map {|n| (ack.include? [n.node]) ? [nil] : [true]}
  end

  # distribute the EDB to each node
  #
  # XXX: this may break coordination protocols that assume the EDB is present
  # before any messages are received.  In order to fix this, we would probably
  # need to globally synchronize to ensure that "timestamp 0" gets "fully
  # evaluated" before any messages can be sent
  bloom :distribute_data do
    initial_data_chan <~ join([deploy_node, initial_data],
                              [deploy_node.uid, initial_data.uid]).map do |n, i|
      [n.node, i.data] if not not_all_in.include? [true] and idempotent [n,i]
    end

    dont_care <= ((initial_data_chan.each do |i|
                     if idempotent i
                       puts "Received all initial data; beginning computation"
                       @new_instance.async_do {
                         safe_eval(i.data.map {|j| j[0].to_s + " <+ " +
                                     j[1].inspect}.join("\n"),
                                   lambda {|s| @new_instance.instance_eval(s)})
                       }
                     end
                   end) and [])

  end
end

class MetaRecv
  include Bud
  include Deployer
end
