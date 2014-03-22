#!/usr/bin/env ruby


require 'json'
require 'fileutils'
require 'socket'
require 'rexml/document'
require 'rexml/xpath'
require 'optparse'


module ActionFigure

	module CallbackRoutine

		def method_added(method_name)
		    return if callbacks.include?(method_name) || callback_callers.include?(method_name) || excepts.include?(method_name)
		    register_caller(method_name)
		end

		# Should be used with :only option
		def before_action(method_name, options = nil)
			raise RuntimeError, "You must assign :only or :except option at minimum to use before_action filter" if options.nil?
		    callbacks << method_name
		    options.each { |key, value|
		        case key
		        when :only
		            onlys << value
		        when :except
		            excepts << value
		        else
		            puts "#{key} doesn't support, skipping parameter"
		        end
		    } unless options.nil?
		end

		def callbacks; @callbacks ||= []; end
		def onlys; @onlys ||= []; end
		def excepts; @excepts ||= [:initialize]; end

		private
		def callback_callers; @callback_callers ||= []; end

		def register_caller(method_name)
		    callback_callers << method_name
		    original_method = instance_method(method_name)

		    define_method(method_name) do |*args, &block|
		        if ((self.class.onlys.empty? || self.class.onlys.include?(method_name)) &&
		                (!self.class.excepts.include?(method_name) || self.class.excepts.empty? ))
		            self.class.callbacks.each do |callback|
		                method(callback).call
		            end
		        end
		        original_method.bind(self).call(*args, &block)
		    end
		end
	end

	class HCatalogSetup

		extend ActionFigure::CallbackRoutine

		attr_accessor :auto_start_webhcat_server, :debug_log_mode_for_pig
		attr_reader :app_version, :is_yarn
		before_action :validate_environment?, :only => :setup
	
		@@hadoop_config_bootstrap_path = "s3://elasticmapreduce/bootstrap-actions/configure-hadoop"
		@@hive_server_ports = { "0.11.0" => 10004, "0.8.1" => 10003, "0.7.1" => 10002, "0.7" => 10001, "0.5" => 10000 }
		@@default_hive_version = "0.11.0"
		@@default_pig_version = "0.11.1.1"

		public
		def initialize
			@app_version = "0.7.2"
			@mnt_path = "/mnt/var"
			@home_path = "/home/hadoop"
			@backup_path = "#{@home_path}/backup_lib"
			@package_path = "#{@home_path}/.versions"
			@extra_info = info_in_json("#{@mnt_path}/lib/instance-controller/extraInstanceData.json")
			@jobflow_info = info_in_json("#{@mnt_path}/lib/info/job-flow.json")
			@instance_info = info_in_json("#{@mnt_path}/lib/info/instance.json")
			@is_master = @instance_info['isMaster']
			@hadoop_version = @jobflow_info['hadoopVersion']
			@master_host = @is_master ? Socket.gethostname: @extra_info['masterHost']
			@master_ip = IPSocket.getaddress(@master_host)
			@hadoop_path = "#{@package_path}/#{@hadoop_version}"
			@is_yarn = @hadoop_version.split(".").first.to_i == 2
			@yarn_hcat_s3_path = "s3://<please-replace-to-your-s3-bucket>/emr/packages/yarn"
			@auto_start_webhcat_server = false
			@debug_log_mode_for_pig = false
			parse_arguments
		end

		def setup
			trace_step "Start Setting HCatalog"
			set_environment_variables
			set_hive_properties
			set_pig_properties
			set_hcatalog
			set_webhcat
		end

		private
		def set_hive_properties
			trace_step "Setting hive properties"
			set_file_permissions
			FileUtils.cp("#{@hive_path}/conf/hive-site.xml", "#{@hive_path}/conf/hive-site.backup") if File.exists?("#{@hive_path}/conf/hive-site.xml")
			config_xml_without_redundancy("#{@hive_path}/conf/hive-site.xml",
				{"hive.metastore.uris" => "thrift://#{@master_ip}:#{hive_server_port}"})
			{"#{@hive_path}/conf/hive-default.xml" => "#{@home_path}/conf/hive-default.xml", 
				"#{@hive_path}/conf/hive-site.xml" => "#{@home_path}/conf/hive-site.xml"}.each { |source, target|
				make_soft_link(source, target)
			}
		end

		def set_file_permissions
			trace_step "Setting file permissions"
			["#{@hcat_path}/bin/hcat", "#{@hcat_path}/sbin/hcat_server.sh",
				"#{@hcat_path}/sbin/update-hcatalog-env.sh", "#{@hcat_path}/sbin/webhcat_config.sh",
				"#{@hcat_path}/sbin/webhcat_server.sh", "#{@hcat_path}/libexec/hcat-config.sh"].each do |file_path|
				FileUtils.chmod(0755, file_path) if File.exist?(file_path)
			end
		end

		def make_soft_link(source, target)
			FileUtils.ln_s(source, target) if ((!File.exists? target) && (File.exists? source))
		end

		def set_pig_properties
			trace_step "Setting Pig properties"
			pig_path = Dir.glob("#{@package_path}/pig-*")
			if (pig_path.empty?)
				return e("No Pig installed")
			end

			pig_ver = `pig -version`
			pig_ver_matches = pig_ver.match(/\d{1,2}\.\d{1,3}\.\d{0,3}.?\d{0,3}/)
			@pig_version = pig_ver_matches.nil? ? @@default_pig_version: pig_ver_matches[0]
			if (File.exists?("#{@package_path}/pig-#{@pig_version}/"))
				@pig_path = "#{@package_path}/pig-#{@pig_version}" 
			else
				return e("Cannot find Pig path")
			end

			FileUtils.mkdir("#{@pig_path}/conf/") unless File.exists?("#{@pig_path}/conf/")
			config_without_redundancy("#{@pig_path}/conf/pig.properties", 
					["#pig.sql.type=hcat", "hcat.bin=/usr/local/hcat/bin/hcat", 
					"pig.logfile=#{@mnt_path}/log/apps/pig.log"],
					["pig.sql.type=hcat", "hcat.bin=#{@hcat_path}/bin/hcat", 
					"pig.logfile=#{@mnt_path}/log/apps/pig.log"])
			config_without_redundancy("#{@pig_path}/conf/pig.properties", ["debug=DEBUG"]) if @debug_log_mode_for_pig
			make_soft_link("#{@pig_path}/conf/pig.properties", "#{@home_path}/pig.properties")
		end
		
		def set_environment_variables
			trace_step "Setting environment variables"
			run_hcat_path = "#{@mnt_path}/run/hcatalog/"
			FileUtils.mkdir(run_hcat_path) unless File.exists?(run_hcat_path)
			configs = is_yarn ? []: ["export HIVE_HOME=$HADOOP_HOME/hive"]
			configs.push("export HCAT_HOME=$HIVE_HOME/hcatalog", 
				"export HCATALOG_HOME=$HCAT_HOME",
				"export WEBHCAT_PREFIX=$HCAT_HOME",
				"export WEBHCAT_CONF_DIR=$HCAT_HOME/conf",
				"export TEMPLETON_HOME=$HCAT_HOME", 
				"export HCAT_PREFIX=$HCAT_HOME",
				"export HCAT_LOG_DIR=#{@mnt_path}/log/apps/",
				"export HCAT_PID_DIR=#{run_hcat_path}",
				"export WEBHCAT_LOG_DIR=#{@mnt_path}/log/apps/",
				"export WEBHCAT_PID_DIR=#{run_hcat_path}",
				"export PATH=$PATH:$HCAT_HOME/bin:$HCAT_HOME/sbin")
			config_without_redundancy("#{@home_path}/.bashrc", configs)
		end

		def set_hcatalog
			trace_step "Setting HCatalog"
			make_soft_link("#{@hcat_path}", "#{@home_path}/hcatalog")
			replace_hcatalog_jars if @is_yarn
			trace_step "Restarting Hive server Nanny service"
			system "sudo /etc/init.d/hive-server-nanny restart"
		end

		def replace_hcatalog_jars
			return unless @is_yarn
			trace_step "Replacing #{@yarn_hcat_libraries} for Hadoop 2.x from #{@yarn_hcatalog_s3_path}"
			backup(@yarn_hcat_libraries, "webhcat")

			@yarn_hcat_libraries.each do |full_path|
				filename = File.basename full_path
				copy_to_local("#{@yarn_hcat_s3_path}/#{filename}", filename)
				if File.exists?(filename)
					FileUtils.mv(filename, File.dirname(full_path))
				else
					raise RuntimeError, "#{filename} does not exist! for replacement"
				end
			end
		end

		def set_webhcat
			trace_step "Setting WebHCat"
			FileUtils.mkdir("#{@hcat_path}/conf") unless File.exists?("#{@hcat_path}/conf")
			FileUtils.cp("#{@hcat_path}/etc/webhcat/webhcat-default.xml", "#{@hcat_path}/conf/webhcat-default.xml") unless File.exists?("#{@hcat_path}/conf/webhcat-default.xml")
			FileUtils.cp("#{@hcat_path}/etc/webhcat/webhcat-log4j.properties", "#{@hcat_path}/conf/webhcat-log4j.properties") unless File.exists?("#{@hcat_path}/conf/webhcat-log4j.properties")


			if @is_yarn
				replace_webhcat_libraries
				insert_hadoop_path_for_webhcat
			end

			user_hdir = "/user/templeton"
			apps_hdir = "/apps/templeton"
			create_hdfs_folder("#{user_hdir}/")
			create_hdfs_folder("#{apps_hdir}/")

			hive_tar = get_hive_tar
			pig_tar = get_pig_tar

			list = {"#{@home_path}/contrib/streaming/hadoop-streaming.jar" => "#{user_hdir}/hadoop-streaming.jar"}
			list[hive_tar] = "#{apps_hdir}/#{hive_tar}" unless hive_tar.nil?
			list[pig_tar] = "#{apps_hdir}/#{pig_tar}" unless pig_tar.nil?
			list.each { |source, target| copy_from_local(source, target); }

			FileUtils.rm("hive-#{@hive_version}.tar.gz") unless hive_tar.nil?
			FileUtils.rm("pig-#{@pig_version}.tar.gz") unless pig_tar.nil?
			FileUtils.rm_rf("pig-#{@pig_version}") if File.exists? "pig-#{@pig_version}"
			FileUtils.cp("#{@hcat_path}/conf/webhcat-site.xml", "#{@hcat_path}/conf/webhcat-site.backup") if File.exists?("#{@hcat_path}/conf/webhcat-site.xml")
			config_xml_without_redundancy("#{@hcat_path}/conf/webhcat-site.xml",
				{"templeton.pig.archive" => "hdfs://#{apps_hdir}/#{pig_tar}",
				"templeton.pig.path" => "#{pig_tar}/bin/pig",
				"templeton.hive.archive" => "hdfs://#{apps_hdir}/#{hive_tar}",
				"templeton.hive.path" => "#{hive_tar}/bin/hive",
				"templeton.streaming.jar" => "hdfs://#{user_hdir}/hadoop-streaming.jar",
				"templeton.hive.properties" => "hive.metastore.local=false,thrift://#{@master_ip}:#{hive_server_port},hive.metastore.sasl.enabled=false",
				"webhcat.proxyuser.hadoop.groups" => "*",
				"webhcat.proxyuser.hadoop.hosts" => "*"})


			unless File.exists? "#{@mnt_path}/log/apps/export"
				trace_step "Creating export directory in log directory for WebHCat"
				FileUtils.mkdir "#{@mnt_path}/log/apps/export"
			end

			if (!File.exists?("#{@mnt_path}/run/hcatalog/webhcat.pid") && @auto_start_webhcat_server)
				trace_step "Starting WebHCat server"
				system "source ~/.bashrc; #{@hcat_path}/sbin/webhcat_server.sh start"
			end
		end

		# Somehow, PATH is unset while hadoop script file is ran by WebHCat on Hadoop 2.x
		# As quick remedy, injecting Hadoop path before it runs.
		def insert_hadoop_path_for_webhcat
			return unless @is_yarn
			trace_step "Adding quick remedy for WebHCat with Hadoop 2.x by exporting PATH in hadoop script"
			export_path = "export PATH=$PATH:$HADOOP_HOME/bin"
			file_path = "#{@home_path}/bin/hadoop"
			FileUtils.cp(file_path, "#{@backup_path}/hadoop")
			content = File.read("#{@backup_path}/hadoop")
			if content.index(export_path).nil?
				File.open(file_path, "w") { |file|
					file.puts content.gsub("# This script runs the hadoop core commands.", 
						"# This script runs the hadoop core commands.\n\n#{export_path}")
				}
				FileUtils.chmod(0755, file_path) if File.exist?(file_path)
			end
		end

		def replace_webhcat_libraries
			return unless @is_yarn
			trace_step "Replacing webhcat libraries for Hadoop 2.x.x"
			lib_dir = "#{@hcat_path}/share/webhcat/svr/lib"
			backup(["#{lib_dir}/pig-0.10.1.jar", "#{lib_dir}/hadoop-core-1.0.3.jar", 
				"#{lib_dir}/hadoop-tools-1.0.3.jar"], "webhcat/lib")
			[ "common", "hdfs",	"mapreduce", "yarn", "tools/lib"].each { |from|
				trace_step "Creating symbolic links from #{@home_path}/share/hadoop/#{from}/*.jar to #{lib_dir}/"
				system "ln -s #{@home_path}/share/hadoop/#{from}/*.jar #{lib_dir}/"
			}
			make_soft_link("#{@package_path}/pig-#{@pig_version}/pig-#{@pig_version}.jar", "#{lib_dir}/pig-#{@pig_version}.jar")
		end

		def get_hive_tar
			create_tar("#{@package_path}/hive-#{@hive_version}/", "hive-#{@hive_version}.tar.gz")
		end

		def get_pig_tar
			if @is_yarn
				name = "#{@package_path}/pig-#{@pig_version}"
			else
				name = "pig-#{@pig_version}"
				FileUtils.cp_r("#{@package_path}/#{name}", name)
				FileUtils.mv("#{name}/bin/pig", "#{name}/bin/pig.backup")
				File.open("#{name}/bin/pig", "w") do |file|
					file.puts "#!/usr/bin/env bash\n\nhadoop jar #{name}.tar.gz/lib/pig/#{name}-amzn.jar $@"
				end
			end
			create_tar("#{name}/", "pig-#{@pig_version}.tar.gz")
		end

		def create_tar(path = nil, filename = nil)
			return nil if path.nil? || filename.nil? || !File.exists?(path)
			trace_step "Creating Tar file as #{filename}"
			return filename if File.exists? filename
			system "tar cvfz #{filename} -C #{path} ."
			filename
		end

		def copy_to_local(source, target)
			if (system("hadoop fs -test -e #{source}") && !File.exists?(target))
				trace_step "Downloding file from #{source} to #{target}"
				system "hadoop fs -copyToLocal #{source} #{target}"
			end
		end

		def copy_from_local(source, target, remove_after_copy = false)
			if (!system("hadoop fs -test -e #{target}") && File.exists?(source))
				trace_step "Uploading file from #{source} to #{target}"
				system "hadoop fs -copyFromLocal #{source} #{target}"
			end
			FileUtils.rm(source) if remove_after_copy
		end
		
		def create_hdfs_folder(path)
			unless (system("hadoop fs -test -e #{path}"))
				trace_step "Creating folder on hdfs #{path}"
				if (@is_yarn)
					system "hadoop fs -mkdir -p #{path}"
				else
					system "hadoop fs -mkdir #{path}"
				end
			end
		end

		def backup(files = [], to = "", move = true)
			trace_step "Backup #{files} => #{to}"
			to_path = "#{@backup_path}/#{to}"
			FileUtils.mkdir_p to_path unless File.exists? to_path
			files.each { |file|
				mv_file file, to_path
			}
		end

		def mv_file(source, target)
			if (File.exists?(source))
				trace_step "Moving file from #{source} to #{target}"
				FileUtils.mv("#{source}", "#{target}")
			else
				trace_step "#{source} does not exists, skip moving file"
			end
		end

		def validate_environment?
			trace_step "Validating envrionment"
			raise RuntimeError, "This is supposed to be ran only on MASTER node" unless @is_master
			raise RuntimeError, "There is no Hive installed, you must install Hive before run this script" unless check_hive_installed
		end

		def check_hive_installed
			return false unless File.exists?("#{@home_path}/hive/")
			@hive_path = "#{@home_path}/hive"
			@hcat_path = "#{@hive_path}/hcatalog"
			@webhcat_path = "#{@hcat_path}"
			hive_ver = `hive -version`
			hive_ver_matches = hive_ver.match(/\d{1,2}\.\d{1,3}\.?\d{1,3}/)
			@hive_full_version = hive_ver.split(" ").last
			@hive_version = hive_ver_matches.nil? ? @@default_hive_version: hive_ver_matches[0]
			@yarn_hcat_libraries = ["#{@hcat_path}/share/hcatalog/hcatalog-core-#{@hive_version}.jar", 
					"#{@hcat_path}/share/webhcat/svr/webhcat-#{@hive_version}.jar", 
					"#{@hcat_path}/share/webhcat/java-client/webhcat-java-client-#{@hive_version}.jar"]
			true
		end

		def hive_server_port
			@@hive_server_ports[@hive_version]
		end

		def trace_step(message)
			puts "\n#{now} ::\t #{message}\n"
		end

		def config_xml_without_redundancy(file_path, key_values, replace_only = false)
			unless File.exists?(file_path)
				if (replace_only)
					raise RuntimeError, "#{file_path} does not exist for replace"
				else
					str_xml = <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
</configuration>
EOF
					File.open(file_path, "w") { |file|
						file.write(str_xml)
					}
				end
			end
			
			doc = REXML::Document.new(File.open(file_path, "r"))
			key_values.each do |key, value|
				if (!edit_xml_value(doc, key, value) && !replace_only)
					add_xml_value(doc, key, value)
				end
			end
			output = File.open(file_path, "w")
			output.puts(doc)
			output.close
		end

		def add_xml_value(doc, name, value)
			doc.root.add(REXML::Document.new "\n\t<property>\n\t\t<name>#{name}</name>\n\t\t<value>#{value}</value>\n\t</property>\n\n")
		end

		def edit_xml_value(doc, name, value)
			name_element = REXML::XPath.first(doc, "//name[text() = '#{name}']")
			return false if name_element.nil? || !name_element.parent?
			value_element = name_element.parent.elements['value']
			return false if value_element.nil?
			value_element.text = value
			true
		end

		def config_without_redundancy(filename, original_lines, config_lines = nil, replace_only = false)
			config_lines ||= original_lines
			if (config_lines.size != original_lines.size) 
				raise RuntimeError, "the number of parameters are different!"
			end
			unless File.exists?(filename)
				if replace_only 
					raise RuntimeError, "#{filename} does not exist for replacing"
				else
					FileUtils.touch(filename)
				end
			end
			
			content = ""
			match_counts = Array.new(original_lines.size, 0)
			File.readlines(filename).each do |line|
				any_match = false
				original_lines.each_with_index do |original_line, index|
					config_line = config_lines[index]
					if (original_line == line.strip)
						if (match_counts[index] == 0)
							content << "#{config_line}"
						end
						match_counts[index] += 1
						any_match = true
					end
				end
				content << line unless any_match
			end
			unless replace_only
				config_lines.each_with_index do |config_line, index|
					content << "\n#{config_line}"	if (match_counts[index] == 0)
				end
			end
			File.open(filename, "w") do |file|
				file.puts content
			end
		end

		# for debugging
		def e(obj, msg = nil)
			puts "###########"
			puts "#{msg}\n" unless msg.nil?
			puts obj.inspect
			puts "###########"
			nil
		end

		def now
			Time.now.getutc
		end

		def info_in_json(path)
			raise RuntimeError, "#{path} does not exist!" unless File.exists?(path)
			JSON.parse(File.read(path))
		end

		def parse_arguments
			@args = []
			@optparse = OptionParser.new do |opts|
				opts.banner = "Usage: #{$0} options"
				opts.on("-h", "--help", "Print help text") do
					puts opts
					exit -1
				end
				opts.on("--start-webhcat-server", "Auto start webhcat server") do |do_start|
					@auto_start_webhcat_server = do_start
				end
				opts.on("--debug-pig-log", "Set Debug log mode for Pig") do |debug|
					@debug_log_mode_for_pig = debug
				end
				opts.on("--hcatalog-s3-path=S3_PATH", "S3 path for replacing HCatalog libraries, this will work only with Hadoop 2.x") do |path|
					@yarn_hcat_s3_path = path
				end
				opts.on("--args") do
					while ARGV.size > 0 do
						@args << ARGV.shift
					end
				end
			end
			@optparse.parse!(ARGV)
		end

	end

end

# Commands
begin
	setup = ActionFigure::HCatalogSetup.new
	setup.setup
rescue Exception => error
	puts "Exit : Error has been raised \n>> #{error}"
	exit 1
end

puts "HCatalog setup is completed successfully by HCatalogSetup -v #{setup.app_version}"
exit 0
