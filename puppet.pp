class myconfig {
  # make sure jdk is installed
  package { 'openjdk-8-jdk':
    ensure => 'installed',
  }
  
  # idempotent exports for setting JAVA_HOME, PATH .. etc
  file { "/etc/profile.d/my_test.sh":
  		content => ' \
			export JAVA_HOME=/lib/jvm/java-8-openjdk-amd64/; \
			export HADOOP_HOME=/home/ubuntu/hadoop; \
			export PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:/home/ubuntu/spark/bin; \
			export HADOOP_CONF_DIR=/home/ubuntu/hadoop/etc/hadoop; \
		'
  }

  # add all host entries
  host { 'group9-master':
    ip => '192.168.2.35',
  }

  host { 'group9-w1':
    ip => '192.168.2.109',
  }
  
  host { 'group9-w2':
    ip => '192.168.2.59',
  }
  
  host { 'group9-w3':
    ip => '192.168.2.87',
  }
  
  host { 'group9-w4':
    ip => '192.168.2.168',
  }
  
  # download hadoop if it doesn't exist (creates condition)
  exec { 'download_hadoop':
  	  command => ' \
	  	  mkdir -p ./hadoop/ ; \
		  wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.2/hadoop-3.3.2.tar.gz; \
		  tar -xvf hadoop-3.3.2.tar.gz -C ./hadoop ; \
	  '
	  cwd     => '/home/ubuntu/',
	  creates => '/home/ubuntu/hadoop/',
	  path    => ['/usr/bin']
  }
  
  # download spark if it doesn't exist (creates condition)
  exec { 'download_spark':
  	  command => ' \
	  	  mkdir -p ./spark/ ; \
		  wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz; \
		  tar -xvf spark-3.2.1-bin-hadoop3.2.tgz -C ./spark ; \
	  '
	  cwd     => '/home/ubuntu/',
	  creates => '/home/ubuntu/spark/',
	  path    => ['/usr/bin']
  }
  
  
  # copy configuration files for both hdfs and spark from the master
  # depends on downloading tasks 
  file { '/home/ubuntu/hadoop/etc/hadoop/yarn-site.xml':
	  ensure   => 'present',
	  source   => 'file:///home/ubuntu/hadoop/etc/hadoop/yarn-site.xml',
	  require => Exec['download_hadoop']
  } 
  
  file { '/home/ubuntu/hadoop/etc/hadoop/hdfs-site.xml':
	  ensure   => 'present',
	  source   => 'file:///home/ubuntu/hadoop/etc/hadoop/hdfs-site.xml',
	  require => Exec['download_hadoop']
  } 
  
  file { '/home/ubuntu/hadoop/etc/hadoop/core-site.xml':
	  ensure   => 'present',
	  source   => 'file:///home/ubuntu/hadoop/etc/hadoop/core-site.xml',
	  require => Exec['download_hadoop']
  } 
  
  file { '/home/ubuntu/spark/conf/spark-defaults.conf':
	  ensure   => 'present',
	  source   => 'file:///home/ubuntu/spark/conf/spark-defaults.conf',
	  require => Exec['download_spark']
  } 
	
  
  


}
