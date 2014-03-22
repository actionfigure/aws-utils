#!/usr/bin/env ruby

require 'fileutils'

module ActionFigure

	class HCatalogPreSetter

		attr_reader :app_version
		@@hd_config_ba_path = "s3://elasticmapreduce/bootstrap-actions/configure-hadoop"

		public
		def initialize
			@home_path = "/home/hadoop"
			@app_version = "0.4"
		end

		def run
			backup_thrift_libs("0.7.0")
			setup_proxy_user("hadoop")
		end

		private
		def backup_thrift_libs(version = "0.7.0")
			backup_path = "#{@home_path}/backup_lib"
			trace_step "Backing up thrift #{version} libraries to #{backup_path}"
			FileUtils.mkdir_p(backup_path) unless File.directory? backup_path

			{ "#{@home_path}/lib/libthrift-#{version}.jar" => "#{backup_path}/libthrift-#{version}.jar",
			"#{@home_path}/lib/libthrift-#{version}-javadoc.jar" => "#{backup_path}/libthrift-#{version}-javadoc.jar" }.each do | source, target|
				mv_file(source, target)
			end
		end

		def mv_file(source, target)
			if (File.exists?(source))
				trace_step "Moving file from #{source} to #{target}"
				FileUtils.mv("#{source}", "#{target}")
			else
				trace_step "#{source} does not exists, skipping moving file"
			end
		end

		def setup_proxy_user(user = "hadoop")
			trace_step "Setting up proxy user as #{user}"
			script = File.basename(@@hd_config_ba_path)
			system "hadoop fs -copyToLocal #{@@hd_config_ba_path} ." unless File.exists? script
			system "ruby #{script} -c hadoop.proxyuser.#{user}.groups=* -c hadoop.proxyuser.#{user}.hosts=* -h dfs.permissions=false -h dfs.permissions.supergroup=hadoop"
		end

		def trace_step(message)
			puts "\n#{Time.now.getutc} ::\t #{message}\n"
		end

	end
end

begin
	# remove libthrift-0.7.0.jar, libthrift-0.7.0-javadoc.jar from classpath
	ba = ActionFigure::HCatalogPreSetter.new
	ba.run
rescue Exception => error
	puts "Exit : error has been raised \n>> #{error}"
	exit 1
end

puts "Bootstrap Action for HCatalog is completed successfully by HCatalogPreSetter -v #{ba.app_version}"
exit 0
