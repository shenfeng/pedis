task :default => :release

desc ""
task :gen do
    sh './deps/deps.sh'
end

desc "Build release binary"
task :release => [:gen] do
    sh 'mkdir -p release && cd release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j'
end

desc "Build debug binary"
task :debug => [:gen] do
    sh 'mkdir -p debug && cd debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j'
end


desc "Clean"
task :clean do
    sh 'rm -rvf *.o network/*.o util/*.o release debug'
end

desc "Run tesst"
task :tests do
    sh "cd tests && python listdb_test.py"
end


desc "copy import_bg_action.py to bi-02"
task :import_bg_action do
    sh "ssh bi-02 'mkdir -p /data/tmp/import_bg_action' && scp -r tests/gen-py tests/import_bg_action.py  tests/import.sh bi-02:/data/tmp/import_bg_action"
end