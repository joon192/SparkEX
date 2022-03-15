from pyspark import SparkConf, SparkContext

# SparkConf은 객체를 초기화 하고 set을 이용하여 설정함.
# SparkConf는 SparkContext를 생성하기 위한 설정 파일
# setAppName()은 Spark 어플리케이션 이름 설정
# setMaster은 로컬 피씨에서 사용하기 위해 local로 작성 만약 "local[N]" 이라고 되어있다면 안의 숫자 N의 의미는 실행할 스레드의 개수를 의미함
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')     # ',' 기준으로 구분하여 데이터를 쪼갬
    age = int(fields[2])         # fields의 2번째 인덱스의 데이터를 age로 변수 설정
    numFriends = int(fields[3])  # fields의 3번째 인덱스의 데이터를 numFriends로 변수 설정
    return (age, numFriends)     # 강제 int화 한 age, numFriends 데이터를 반환

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")  # textFile은 HDFS, 텍스트파일을 포함한 디렉토리로부터 RDD를 생성하는 방법
rdd = lines.map(parseLine)  # lines 데이터를 parkseLine() 함수를 사용하여 반환된 age, numFreiends를 한번에 한줄씩 RDD로 변환하기 위하여 map 기능 사용
#new_rd = rdd.collect()

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))  # mapValues를 사용하면 Key 값은 유지하고 Value만 변환 할 수 있다. reduceByKey를 사용하여 핵심 값인 연령값을 모음
tot = totalsByAge.collect()
print(tot)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
