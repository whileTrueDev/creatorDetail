
// 정렬된채로 전달받음.
const getIQ = ({ streamData }) => {
  const len = streamData.length;
  const median = getMedian({ streamData });
  let Q1;
  let Q3;
  if (len % 2 === 0) {
    // 짝수이므로, len과 len -1 번째 값의 평균을 중앙값으로 사용한다.

    Q1 = getMedian({ streamData: streamData.slice(0, len / 2) });
    Q3 = getMedian({ streamData: streamData.slice(len / 2 + 1) });
  } else {
    Q1 = getMedian({ streamData: streamData.slice(0, Math.floor(len / 2)) });
    Q3 = getMedian({ streamData: streamData.slice(Math.floor(len / 2) + 1) });
  }
  const IQR = Q3 - Q1;
  const outline = Q1 - 1.5 * (IQR);
  return {
    median,
    Q1,
    Q3,
    IQR,
    outline
  };
};

// 방송이 진행된 요일별 퍼센트를 구한다.
const getDayofWeekPercent = () => {
  const dayweekQuery = `
  SELECT count(*), weekday(time)
  FROM twitchStreamDetail AS A
  LEFT JOIN twitchStream AS B
  ON A.streamId = B.streamId
  WHERE B.streamerId = '147356756'
  group by weekday(time)`;
};

const getMedian = ({ streamData }) => {
  streamData.sort((a, b) => a.airtime - b.airtime);
  const average = (data) => data.reduce((sum, value) => sum + value) / data.length;
  const len = streamData.length;
  let median = 0;
  if (len % 2 === 0) {
    // 짝수이므로, len과 len -1 번째 값의 평균을 중앙값으로 사용한다.
    // console.log(`기준은 ${len / 2}`);
    median = average([streamData[len / 2].airtime, streamData[len / 2 + 1].airtime]);
  } else {
    // console.log(`기준은 ${len / 2}`);
    median = streamData[Math.floor(len / 2)].airtime;
  }
  return median;
};

const stddev = values => Math.sqrt(average(values.map(value => (value - average(values)) ** 2)));


const getHeatmapData = ({ connection, creatorId }) => new Promise((resolve, reject) => {
  const date = new Date();
  date.setDate(date.getDate() - 1);

  const selectQuery = `
  SELECT DATE_FORMAT(date(time), "%y-%m-%d") as date , hour(time) as time, sum(viewer) as viewer
  FROM twitchStreamDetail AS A
  LEFT JOIN twitchStream AS B 
  ON A.streamId = B.streamId
  WHERE B.streamerId = ?
  AND date(time) < ?
  GROUP BY date(time), hour(time)
  ORDER BY date(time) desc, hour(time) asc
  `;

  doConnectionQuery({ connection, queryState: selectQuery, params: [creatorId, date] })
    .then((row) => {
      // 데이터 들고와서 일별로 빈시간을 채우는 전처리 시작.
      const heatmapData = preprocessingHeatmapData(row);
      const jsonHeatmapData = JSON.stringify({ data: heatmapData });
      return jsonHeatmapData;
    })
    .then((jsonHeatmapData) => {
      const queryState = `
      UPDATE creatorDetail 
      SET viewerHeatmapData = ?
      WHERE creatorId = ? 
      `;
      doTransacQuery({ connection, queryState, params: [jsonHeatmapData, creatorId] })
        .then(() => {
          resolve();
        })
        .catch((error) => {
          console.log(`${creatorId}의 데이터를 저장하는 과정`);
          reject(error);
        });
    })
    .catch((err) => {
      console.log(`${creatorId}의 데이터를 저장하는 과정`);
      reject(err);
    });
});


const preprocessingHeatmapData = (row) => {
  // 사전에 날짜별로 시간별로 정렬된 상태이므로
  const uniqueDateList = row.reduce((result, item) => {
    if (!result.includes(item.date)) {
      // 데이터가 존재하지 않으면 집어넣는다.
      result.push(item.date);
    }
    return result;
  }, []);

  // 날짜를 7개만 사용하도록 한다.
  if (uniqueDateList.length > 7) {
    uniqueDateList.splice(7);
  }

  // 날짜가 나왔으므로 날짜 / 시간별로 template 생성.
  const outData = [];
  uniqueDateList.forEach((date) => {
    [...Array(24).keys()].forEach((i) => {
      // date와 time이 일치하는 값을 가져온다.
      const matchIndex = row.findIndex((x) => x.date === date && x.time === i);
      let data = {
        date,
        time: i,
        viewer: 0
      };
      if (matchIndex !== -1) {
        data = {
          date,
          time: i,
          viewer: row[matchIndex].viewer
        };
      }
      outData.push(data);
    });
  });
  return outData;
};



const getfollowerZerodoQueryList = () => new Promise((resolve, reject) => {
  const selectQuery = `
  select creatorId
  from creatorDetail
  where followers = 0
  `;
  doQuery(selectQuery, [])
    .then((row) => {
      const creatorList = row.result.map((element) => element.creatorId);
      resolve(creatorList);
    })
    .catch((err) => {
      console.log('creator list를 가져오는 과정');
      console.log(err);
      resolve([]);
    });
});


const dividedZeroUpdate = async () => {
  console.log('follower zero update start.');
  const creatorList = await getfollowerZerodoQueryList();
  const turns = Math.ceil(creatorList.length / 30);
  const targetList = [];
  for (let i = 0; i < turns;) {
    const targets = creatorList.splice(0, 30);
    targetList.push(targets);
    i += 1;
  }

  return new Promise((resolve, reject) => {
    forEachPromise(targetList, dividedGetFollower)
      .then(() => {
        resolve();
      });
  });
};