require 'rubygems'
require 'AWS'
require 'pp'
require 'net/ssh'
require 'net/scp'
require 'bud'
require 'bud/deploy/deployer'

module EC2Deploy
  include Deployer

  state do
    table :access_key_id, [] => [:key]
    table :secret_access_key, [] => [:key]
    table :image_id, [] => [:img]
    table :key_name, [] => [:name]
    table :ec2_key_location, [] => [:loc]
    table :ec2_conn, [] => [:conn]
    table :ec2_insts, [] => [:insts]
    table :reservation_id, [] => [:rid]
    periodic :spinup_timer, 6
    scratch :the_reservation, [] => [:reservation]
    scratch :the_reservation_next, [] => [:reservation]
    scratch :node_up, [:node] => [:bool]
    table :node_ssh, [:node_tuple] => [:ssh]
    table :init_dir, [] => [:dir]
    table :temp_node, [:uid, :node, :localip]
    table :all_up, [:bool]
    table :ruby_command, [] => [:cmd]
    table :deploy_node, [:uid] => [:node]
    scratch :deploy_node_count, [:num]
  end

  bootstrap do
    image_id <= [["ami-bec73ad7"]]
  end

  bloom :spinup do
    ec2_conn <= join([access_key_id, secret_access_key]).map do
      if idempotent [:ec2_comm]
        puts "Creating EC2 connection"
        [AWS::EC2::Base.new(:access_key_id => access_key_id[[]].key,
                            :secret_access_key => secret_access_key[[]].key)]
      end
    end

    ec2_insts <= join([image_id, node_count, key_name, ec2_conn]).map do
      if idempotent [:ec2_insts]
        puts "Starting up EC2 instances"
        [ec2_conn[[]].conn.run_instances(:image_id => image_id[[]].img,
                                         :min_count => node_count[[]].num,
                                         :max_count => node_count[[]].num,
                                         :key_name => key_name[[]].name)]
      end
    end

    the_reservation <= join([spinup_timer, ec2_conn, ec2_insts]).map do |t,c,i|
      if idempotent [[:the_reservation, t.val]] and not all_up.include? [true]
        puts "Checking on the reservation"
        [ec2_conn[[]].conn.describe_instances()["reservationSet"]["item"].find do |i|
           i["reservationId"] == ec2_insts[[]].insts["reservationId"]
         end]
      end
    end

    # ULTRA HACK (cuz we don't have an upsert operator)
    the_reservation_next <+ the_reservation

    node_up <= ((join([ec2_insts, the_reservation]).map do
                   if not all_up.include? [true]
                     the_reservation[[]].reservation["instancesSet"]["item"].map do |i|
                       [i, i["instanceState"]["code"] == "16"]
                     end
                   end
                 end)[0] or [])


    all_up <+ node_up.map do
      if node_up.find {|n| n.bool == false} == nil and node_up.find {|n| n.bool == true} != nil
        if idempotent [:nodes_all_up]
          puts "Nodes are all up"
          [true]
        end
      end
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
      break ((puts  ((0..(the_reservation_next[[]].reservation["instancesSet"]["item"].size-1)).to_a.zip(the_reservation_next[[]].reservation["instancesSet"]["item"].map {|i| [i["ipAddress"], i["privateIpAddress"]] })).map {|n,ips| [n, ips[0] + ":54321", ips[1] + ":54321"]}.inspect) or 
             ((0..(the_reservation_next[[]].reservation["instancesSet"]["item"].size-1)).to_a.zip(the_reservation_next[[]].reservation["instancesSet"]["item"].map {|i| [i["ipAddress"], i["privateIpAddress"]]})).map {|n,ips| [n, ips[0] + ":54321", ips[1] + ":54321"]})
    end

    # For each node, upload the file or directory in init_files, then execute
    # the ruby_command
    deploy_node <= join([temp_node, init_dir, ruby_command]).map do |t, i, r|
      if idempotent [[:node_startup, t.node]]
        ip = t.node.split(":")[0]
        port = t.node.split(":")[1]
        stdout_file = File.new(ip + "." + port + ".stdout", 'a')
        stderr_file = File.new(ip + "." + port + ".stderr", 'a')

        # Upload the initial file (or directory)
        10.times do
          begin
            Net::SCP.upload!(ip, 'ec2-user', init_dir[[]].dir, '/home/ec2-user',
                             :recursive => true, :ssh => {:keys =>
                               [ec2_key_location[[]].loc]})

            break true
          rescue Exception
            puts "retrying ssh (this is normal)..."
            sleep 6
            next
          end
        end or raise "Failed to connect to EC2 ssh after 10 retries"

        # Run the initial deployment command
        Net::SSH.start(ip, 'ec2-user', :keys => [ec2_key_location[[]].loc]) do |session|
          session.open_channel do |channel|
            channel.request_pty do |_, success|
              raise "PTY Error" unless success
            end
            channel.exec('nohup ' + ruby_command[[]].cmd + ' ' + t.localip + ' ' +
                         ip + ' >metarecv.out 2>metarecv.err </dev/null') do |_, success|
              channel.on_data do |_, data|
                stdout_file.print(data)
              end
              channel.on_extended_data do |_, _, data|
                stderr_file.print(data)
              end
            end
          end
        end

        puts "Done commands and upload:" + t.inspect
        # XXX: Wait for everything to start up on the remote node
        sleep 6
        [t.uid, t.node]
      end
    end

  end

  bloom :all_nodes do
    # Does num_nodes deploy_nodes exist?  If so, put them all in node atomically
    deploy_node_count <= deploy_node.group(nil, count)
    stdio <~ deploy_node_count.map {|d| ["deploy node count: " + d.inspect]}
    node <= join([deploy_node_count, node_count, deploy_node],
                 [deploy_node_count.num, node_count.num]).map do |_, _, d|
      d
    end
  end

end
