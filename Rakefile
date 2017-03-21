task :default => :release

desc ""
task :gen do
    sh './deps/deps.sh'
end

desc "Build release binary"
task :release => [:gen] do
    sh 'mkdir -p release && cd release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j'
end

desc "Clean"
task :clean do
    sh 'rm -rvf *.o network/*.o util/*.o release debug'
end

desc "Run tesst"
task :tests do
    sh "cd tests && python listdb_test.py"
end