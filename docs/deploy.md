# Deployment

Bud provides support for deploying a program onto a set of Bud instances.  At the moment, two types of deployments are supported: fork-based local deployment and EC2 deployment.  Intuitively, you include the module corresponding to the type of deployment you want into a Bud class, which you instantiate and run on a node called the "deployer."  The deployer then spins up a requested number of Bud instances and distributes initial data.

First, decide which type of deployment you want to use.

## Fork-based Local Deployment

To use fork-based deployment, you'll need to require it in your program:

    require 'bud/deploy/forkdeploy'

Don't forget to include it in your class:

    include ForkDeploy

The next step is to declare how many nodes you want to the program to be spun up on.  You need to do this in a `deploystrap` block.  A `deploystrap` block is run before `bootstrap`, and is only run for a Bud class that is instantiated with the option `:deploy => true`.  As an example:

    deploystrap do
      num_nodes <= [[2]]
    end

Fork-based deployment will spin up `num_nodes` local processes, each containing one Bud instance, running the class that you include `ForkDeploy` in.  The deployment code will populate a binary collection called `node`; the first columm is a "node ID" -- a distinct integer from the range `[0, num_nodes - 1]` -- and the second argument is an "IP:port" string associated with the node.  Nodes are spun up on ephemeral ports, listening on "localhost".

Now, you need to define how you want the initial data to be distributed.  You can do this, for example, by writing (multiple) rules with `initial_data` in the head.  These rules can appear in any `bloom` block in your program. The schema of `initial_data` is as follows: [node ID, relation name as a symbol, list of tuples].

For example, to distribute the IP address and port of the "deployer" to all of the other nodes in a relation called `master`, you might decide to write something like this:

    initial_data <= node {|n| [n.uid, :master, [[ip_port]]]}

Note that the relation (`master` in this case) cannot be a channel -- you may only distribute data to scratches and tables.  Initial data is transferred only after _all_ nodes are spun up; this ensures that initial data will never be lost because a node is not yet listening on a socket, for example.  Initial data is transmitted atomically to each node; this means that on each node, _all_ initial data in _all_ relations will arrive at the same Bud timestep.  However, there is no global barrier for transfer of initial data.  For example, if initial data is distributed to nodes 1 and 2, node 1 may receive its initial data first, and then send subsequent messages on channels to node 2 which node 2 may receive before its initial data.

The final step is to add `:deploy => true` to the instantiation of your class.  Note that the fork-based deployer will spin up nodes without `:deploy => true`, so you don't forkbomb your system.


## EC2 Deployment

To use EC2 deployment you'll need to require it in your program:

    require 'bud/deploy/ec2deploy'

Don't forget to include it in your class:

    include EC2Deploy

As in local deployment, you'll need to define `num_nodes` in a `deploystrap` block.  Additionally in the `deploystrap` block, you need to define the following relations: `ruby_command`, `init_dir`, `access_key_id`, `secret_access_key`, `key_name`, `ec2_key_location`.  `ruby_command` is the command line to run on the EC2 nodes.  For example, if you want to run a file called `test.rb` on your EC2 nodes, you'd put:

    ruby_command <= [["ruby test.rb"]]

Note that whatever file you specify here _must_ take three arguments.  Here's the recommended boilerplate that you use for the file you want to deploy, assuming `Test` is the name of your class:

    ip, port = ARGV[0].split(':')
    ext_ip, ext_port = ARGV[1].split(':')
    Test.new(:ip => ip,
             :ext_ip => ext_ip,
             :port => port,
             :deploy => not ARGV[2]).run_fg

`init_dir` is the directory that contains all of the Ruby files you want to deploy.  Alternatively, `init_dir` may be the single filename you include in your `ruby_command`.  If it is a directory, it must contain the file you execute in your `ruby_command`.  Unless you're doing something particularly fancy, you'll usually set `init_dir` to ".":

    init_dir <= [["."]]

This recursively copies all directories and files rooted at the current working directory.  `access_key_id` is your EC2 access key ID, and `secret_access_key` is your EC2 secret access key.

    access_key_id <= [["XXXXXXXXXXXXXXXXXXXX"]]
    secret_access_key <= [["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"]]

`key_name` is the name of the keypair you want to use to SSH in to the EC2 instances.  For example, if you have a keypair named "bob", you'd write:

    key_name <= [["bob"]]

Finally, `ec2_key_location` is the path to the private key of the `key_name` keypair.  For example:

    key_name <= [["/home/bob/.ssh/ec2"]]

EC2 deployment will spin up `num_nodes` instances (using defaults) on EC2 using a pre-rolled Bud AMI based on Amazon's 32-bit Linux AMI (`ami-8c1fece5`).  Each instance contains one Bud instance, which runs the `ruby_command`.  Like before, the deployment code will populate a binary relation called `node`; the first argument is a "node ID" -- a distinct integer from the range [0, num_nodes - 1] -- and the second argument is an "IP:port" string associated with the node.  Nodes are currently spun up on fixed port 54321.

Defining initial data works exactly the same way with EC2 deployment as it does with local deployment.

There is a slight catch with EC2 deployment.  Sometimes EC2 tells us that the nodes have all started up, but really, one or more nodes will never start up.  Currently, in this scenario, deployment exceeds the maximum number of ssh retries, and throws an exception.

Note that EC2 deployment does *not* shut down the EC2 nodes it starts up under any circumstances.  This means you must use some alternate means to shut down the nodes, such as logging onto the EC2 web interface and terminating the nodes.

## Examples

Check out the `examples/deploy` directory in Bud.  There is a simple token ring example that establishes a ring involving 10 nodes and sends a token around the ring continuously.  This example can be deployed locally:

    ruby tokenring-local.rb

or on EC2:

    ruby tokenring-ec2.rb local_ip:local_port ext_ip true

Note that before running `tokenring-ec2`, you must create a "keys.rb" file that contains `access_key_id`, `secret_access_key`, `key_name` and `ec2_key_location`.

Output will be displayed to show the progress of the deployment.  Be patient, it may take a while for output to appear.  Once deployment is complete and all nodes are ready, each node will display output indicating when it has the token.  All output will be visible for the local deployment case, whereas only the deployer node's output will be visible for the EC2 deployment case (stdout of all other nodes is materialized to disk).