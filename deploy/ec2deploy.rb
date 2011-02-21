require 'rubygems'
require 'AWS'
require 'pp'
#require 'net/ssh'
#require 'net/scp'
require 'bud'
require 'open3'

module EC2Deploy
  include BudModule

  def state
    table :max_count, [] => [:num]
    table :min_count, [] => [:num]
    table :access_key_id, [] => [:key]
    table :secret_access_key, [] => [:key]
    table :image_id, [] => [:img]
    table :key_name, [] => [:name]
    table :ec2_key_location, [] => [:loc]
    table :ec2_conn, [] => [:conn]
    table :ec2_insts, [] => [:insts]
    table :reservation_id, [] => [:rid]
    table :init_command, [] => [:cmd]
    periodic :spinup_timer, 6
    scratch :the_reservation, [] => [:reservation]
    scratch :the_reservation_next, [] => [:reservation]
    scratch :node_up, [:node] => [:bool]
    table :node_ssh, [:node_tuple] => [:ssh]
    table :init_dir, [] => [:dir]
    table :temp_node, [:uid, :node, :localip]
    table :all_up, [:bool]
  end

  state {
    table :max_count, [] => [:num]
    table :min_count, [] => [:num]
    table :access_key_id, [] => [:key]
    table :secret_access_key, [] => [:key]
    table :image_id, [] => [:img]
    table :key_name, [] => [:name]
    table :ec2_key_location, [] => [:loc]
    table :ec2_conn, [] => [:conn]
    table :ec2_insts, [] => [:insts]
    table :reservation_id, [] => [:rid]
    table :init_command, [] => [:cmd]
    periodic :spinup_timer, 6
    scratch :the_reservation, [] => [:reservation]
    scratch :the_reservation_next, [] => [:reservation]
    scratch :node_up, [:node] => [:bool]
    table :node_ssh, [:node_tuple] => [:ssh]
    table :init_dir, [] => [:dir]
    table :temp_node, [:uid, :node, :localip]
    table :all_up, [:bool]
  }

  def bootstrap
    super
    image_id <= [["ami-76f0061f"]]
    init_command <= [["sudo yum -y install rubygems ruby-devel gcc-c++ && sudo gem update --system --no-ri --no-rdoc && sudo gem install deploy/bud-0.0.1.gem --no-ri --no-rdoc" ]]

    # XXX: this is kind of a hack.  once we publish the bud gem online, we
    # won't need to copy it, and hopefully we can integrate the metarecv and
    # deploy stuff into the bud gem to avoid transferring this stuff
    init_dir <= [["/home/wrm/devel/bud/deploy"]]
  end

  declare
  def spinup

    # HACK HACK: join & map functions shouldn't be required
    ec2_conn <= join([access_key_id, secret_access_key]).map do
      (puts "Creating EC2 connection") or
        [AWS::EC2::Base.new(:access_key_id => access_key_id[[]].key,
                            :secret_access_key => secret_access_key[[]].key)] if idempotent [:ec2_conn]
    end

    # HACK HACK: join & map function shouldn't be required
    ec2_insts <= join([image_id, min_count, max_count, key_name, ec2_conn]).map do
      (puts "Starting up EC2 instances") or
        [ec2_conn[[]].conn.run_instances(:image_id => image_id[[]].img,
                                         :min_count => min_count[[]].num,
                                         :max_count => max_count[[]].num,
                                         :key_name => key_name[[]].name)] if idempotent [:ec2_insts]
    end

    the_reservation <= join([spinup_timer, ec2_conn, ec2_insts]).map do |t,c,i|
      ((puts "Checking on the reservation") or
       [ec2_conn[[]].conn.describe_instances()["reservationSet"]["item"].find do |i|
          i["reservationId"] == ec2_insts[[]].insts["reservationId"]
        end]) if idempotent [[:the_reservation, t.val]] and not all_up.include? [true]
    end

    # ULTRA HACK (cuz we don't have an upsert operator)
    the_reservation_next <+ the_reservation

    # XXX: maybe less than the max got spun up
    # HACK HACK: join & map function shouldn't be required
    node_up <= join([ec2_insts, the_reservation]).map do
      the_reservation[[]].reservation["instancesSet"]["item"].map do |i|
        [i, i["instanceState"]["code"] == "16"]
      end[0] if not all_up.include? [true]
    end


    # HACK HACK: map function shouldn't be required
    all_up <+ node_up.map do
      (node_up.find {|n| n.bool == false} == nil and node_up.find {|n| n.bool == true} != nil) ? ((puts "Nodes are all up") or [true]) : nil
    end

    # dole out IPs w/ port 54321
    # HACK HACK: join & map shouldn't be required; also, the_reservation_next
    # is a massive hack because we don't have an upsert operator; also, there's
    # some serious duplication of code here, because we apparently can't have
    # assignment statements in the body of a Bud rule; also, this is a constant
    # that contains a list, rather than a list, because we can't apparently put
    # a "[0] or [nil]" at the end of this rule to peel off the extra level of
    # array
    temp_node <= join([all_up, the_reservation_next]).map do
      break ((puts  ((1..(the_reservation_next[[]].reservation["instancesSet"]["item"].size)).to_a.zip(the_reservation_next[[]].reservation["instancesSet"]["item"].map {|i| [i["ipAddress"], i["privateIpAddress"]] })).map {|n,ips| [n, ips[0] + ":54321", ips[1] + ":54321"]}.inspect) or 
             ((1..(the_reservation_next[[]].reservation["instancesSet"]["item"].size)).to_a.zip(the_reservation_next[[]].reservation["instancesSet"]["item"].map {|i| [i["ipAddress"], i["privateIpAddress"]]})).map {|n,ips| [n, ips[0] + ":54321", ips[1] + ":54321"]})
    end

    # for each SSH connection, upload all the files in init_files, then
    # execute all the init_commands
    node <= join([temp_node, init_dir, init_command]).map do |t, i, c|
      (while not system 'scp -r -o "StrictHostKeyChecking no" -i ' + ec2_key_location[[]].loc + ' ' + init_dir[[]].dir + ' ec2-user@' + t.node.split(":")[0] + ':/home/ec2-user'
         sleep 6
       end) or
        (while not system 'ssh -t -o "StrictHostKeyChecking no" -i '+ ec2_key_location[[]].loc + ' ec2-user@' + t.node.split(":")[0] + ' "' + init_command[[]].cmd + '"'
           sleep 6
         end) or
        (while not system 'ssh -f -o "StrictHostKeyChecking no" -i '+ ec2_key_location[[]].loc + ' ec2-user@' + t.node.split(":")[0] + ' "cd deploy; nohup ruby metarecv.rb ' + t.localip + ' > metarecv.out 2> metarecv.err < /dev/null"'
           sleep 6
         end) or
        ((puts "Done commands and upload:" + t.inspect) or ((sleep 6) and [t.uid, t.node])) if idempotent [:node]
    end

  end
end
