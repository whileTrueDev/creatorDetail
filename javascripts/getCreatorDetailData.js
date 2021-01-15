/**
 * 온애드의 "트위치" 크리에이터 방송에 대한 지표 분석 프로그램입니다.
 * @author chanuuuuu
 */
const pool = require('../model/connectionPool');
const { doQuery, doConnectionQuery, doTransacQuery } = require('../model/doQuery');
const updateFollower = require('./updateFollower');

let startDate;
const ROWS_PER_ONE_HOUR = 3 * 6; // 트위치 데이터의 경우 1시간에 18회 수집한다. (10분당 3번 요청 -> 60분이면 3 * 6)
const forEachPromise = (items, fn) => items.reduce((promise, item) => promise.then(() => fn(item)), Promise.resolve());

const getViewerAirtime = ({
  connection, creatorId, creatorTwitchOriginalId
}) => {
  const selectQuery = `
  SELECT  B.streamId, AVG(viewer) AS viewer, COUNT(*) AS airtime, MAX(viewer) AS peakview
  FROM
  (SELECT streamId
  FROM twitchStream 
  WHERE streamerId = ? AND startedAt > ?) AS B
  LEFT JOIN twitchStreamDetail AS A
  ON A.streamId = B.streamId
  GROUP BY streamId
  `;

  const removeOutliner = ({ streamData }) => {
    const cutStreamData = streamData.reduce((acc, element) => {
      // 한시간 이하의 방송은 이상치이므로 제거한다.
      if (element.airtime > ROWS_PER_ONE_HOUR) { // 트위치 데이터의 경우 1시간에 18회 수집한다.
        acc.push(element);
      }
      return acc;
    }, []);

    if (cutStreamData.length === 0) {
      return {};
    }
    const viewerList = [];
    const airtimeList = [];
    const peakList = [];
    const average = (data) => data.reduce((sum, value) => sum + value) / data.length;
    
    cutStreamData.forEach(({ viewer, airtime, peakview }) => {
      peakList.push(peakview);
      airtimeList.push(airtime);
      viewerList.push(viewer);
    });

    // 시간의 경우, ROW의 갯수를 COUNT하여 값을 가져오는데, 10분당 3개를 찍으므로 3으로 나눈다.
    // 3개당 10분 이므로, 3을 나눈 갯수 * 10이 실제시간이 되고, 60으로 나누면 시간이 나오게 된다. 평균 방송시간이 나오게 된다.
    const airtime = Number((average(airtimeList) / (ROWS_PER_ONE_HOUR)).toFixed(1));
    const viewer = (Math.round(average(viewerList)));
    const peakview = Math.max(...peakList);

    return {
      airtime, viewer, peakview
    };
  };

  return new Promise((resolve, reject) => {
    doConnectionQuery({ connection, queryState: selectQuery, params: [creatorTwitchOriginalId, startDate] })
      .then((row) => {
        if (row.length > 0) {
          const viewData = removeOutliner({ streamData: row });
          resolve(viewData);
        } else {
          resolve({});
        }
      })
      .catch((err) => {
        console.error(err);
        console.error(`ERROR - ${creatorId}의 시청자수, 방송시간을 구하는 과정`);
        resolve({});
      });
  });
};

const getContentsPercent = ({
  connection, creatorId, creatorTwitchOriginalId
}) => {
  const selectQuery = `
  SELECT gameName, gameNameKr, C.gameId, timecount
  FROM (SELECT gameId, count(*) as timecount
  FROM twitchStreamDetail AS A
  LEFT JOIN 
  (
  SELECT streamId, streamerId
  FROM twitchStream
  WHERE startedAt > ?
  ) AS B 
  ON A.streamId = B.streamId
  WHERE B.streamerId = ?
  GROUP BY gameId) AS C
  INNER JOIN twitchGame
  ON C.gameId = twitchGame.gameId
    `;

  const preprocessing = (percentData) => {
    const outputData = percentData;
    const sum = percentData.reduce((a, b) => a + b.timecount, 0);

    percentData.forEach((element, index) => {
      const percent = Number((element.timecount / sum).toFixed(2));
      outputData[index].percent = percent;
    });

    outputData.sort((a, b) => b.timecount - a.timecount);

    let cumsum = 0;
    const returnData = outputData.reduce((result, element) => {
      if (cumsum <= 0.8) {
        cumsum += element.percent;
        result.push({
          gameName: element.gameNameKr ? element.gameNameKr : element.gameName,
          percent: element.percent
        });
      }
      return result;
    }, []);

    returnData.push({
      gameName: '기타',
      percent: Number((1 - cumsum).toFixed(2))
    });

    return returnData;
  };

  return new Promise((resolve, reject) => {
    doConnectionQuery({ connection, queryState: selectQuery, params: [startDate, creatorTwitchOriginalId] })
      .then((row) => {
        if (row.length > 0) {
          const contentsGraphData = preprocessing(row);
          const content = contentsGraphData[0].gameName;
          resolve({ content, contentsGraphData });
        } else {
          console.log(`${creatorId}의 방송시간대를 구하는 과정`);
          resolve({ content: '', contentsGraphData: {} });
        }
      })
      .catch((err) => {
        console.error(err);
        console.error(`ERROR - ${creatorId}의 방송시간대를 구하는 과정`);
        resolve({ content: '', contentsGraphData: {} });
      });
  });
};

const getOpenHourPercent = ({ connection, creatorId, creatorTwitchOriginalId }) => {
  const getTimeName = (time) => {
    if (time >= 0 && time < 6) {
      return '새벽';
    }
    if (time >= 6 && time < 12) {
      return '오전';
    }
    if (time >= 12 && time < 18) {
      return '오후';
    }
    if (time >= 18 && time < 24) {
      return '저녁';
    }
  };

  const countTime = ({ timeData }) => {
    const timeDict = {
      새벽: 0,
      오전: 0,
      오후: 0,
      저녁: 0,
    };
    timeData.forEach((element) => {
      const timeName = getTimeName(element.hours);
      timeDict[timeName] += element.sumtime;
    });
    const timeList = Object.keys(timeDict).map((dateName) => {
      const count = timeDict[dateName];
      return {
        name: dateName,
        count
      };
    });
    timeList.sort((a, b) => b.count - a.count);
    return `${timeList[0].name}, ${timeList[1].name}`;
  };

  const normalization = ({ timeData }) => {
    let outData = [];

    if (timeData.length !== 24) {
      outData = [...Array(24).keys()].map((i) => ({
        hours: i,
        sumtime: 0
      }));
      timeData.forEach((element) => {
        const hour = element.hours;
        outData[hour].sumtime = element.sumtime;
      });
    } else {
      outData = timeData;
    }


    // 일단 크기순으로 sorting (24개의 원소이므로 굉장히 작은 연산이다.)
    outData.sort((a, b) => a.sumtime - b.sumtime);

    const min = outData[0].sumtime;
    const max = outData[23].sumtime;
    outData = outData.map((element) => {
      const sumtime = Number(((element.sumtime - min) / (max - min)).toFixed(2));
      return {
        ...element,
        sumtime
      };
    });

    outData.sort((a, b) => a.hours - b.hours);
    const returArray = outData.concat(outData.splice(0, 6));

    return returArray;
  };

  const timeQuery = `
  SELECT count(*) as sumtime, hour(time) as hours
  FROM twitchStreamDetail AS A
  LEFT JOIN 
  (
  SELECT streamId, streamerId
  FROM twitchStream
  WHERE startedAt > ?
  ) AS B 
  ON A.streamId = B.streamId
  WHERE B.streamerId = ?
  group by hour(time)`;

  return new Promise((resolve, reject) => {
    doConnectionQuery({ connection, queryState: timeQuery, params: [startDate, creatorTwitchOriginalId] })
      .then((row) => {
        if (row.length > 0) {
          const openHour = countTime({ timeData: row });
          const timeGraphData = normalization({ timeData: row });
          resolve({ openHour, timeGraphData });
        } else {
          console.log(`${creatorId}의 방송시간대를 구하는 과정`);
          resolve({ openHour: '', timeGraphData: {} });
        }
      })  
      .catch((err) => {
        console.log(`${creatorId}의 방송시간대를 구하는 과정`);
        resolve({ openHour: '', timeGraphData: {} });
      });
  });
};

// 해당 크리에이터의 일간 평균 클릭수
const getClickPercent = ({ connection, creatorId }) => {
  const clickQuery = `
  select ROUND(avg(counts), 2) as counts
  from (select count(*) as counts, date(date)
  from landingClickIp
  where creatorId = ?
  and landingClickIp.type = 2
  group by date(date)) as C
  `;

  return new Promise((resolve, reject) => {
    doConnectionQuery({ connection, queryState: clickQuery, params: [creatorId] })
      .then((row) => {
        if (row.length > 0) {
          const clickCount = row[0].counts;
          resolve({ clickCount });
        } else {
          console.log(`${creatorId}의 클릭률을 구하는 과정`);
          resolve({ clickCount: 0 });
        }
      })
      .catch((err) => {
        console.log(`${creatorId}의 클릭률을 구하는 과정`);
        resolve({ clickCount: 0 });
      });
  });
};

// 2. API로 수집된 실제 방송 시간
const getAirtime = ({ connection, creatorId, creatorTwitchOriginalId }) => new Promise((resolve, reject) => {
  const selectQuery = `
  SELECT  COUNT(*) AS airtime
  FROM twitchStreamDetail AS A
  LEFT JOIN twitchStream AS B 
  ON A.streamId = B.streamId
  WHERE B.streamerId = ?
  AND time > ?
  `;
  // 실제 노출시간의 경우, 반드시 3으로 나누어줘야함.
  // Number((average(airtimeList) / (6 * 3)).toFixed(1));

  doConnectionQuery({ connection, queryState: selectQuery, params: [creatorTwitchOriginalId, startDate] })
    .then((row) => {
      const { airtime } = row[0];
      resolve(Number(Math.round(Number(airtime) / 3)));
    })
    .catch((err) => {
      console.log(`${creatorId}의 실제 방송 시간을 구하는 과정`);
      console.log(err);
      resolve(0);
    });
});

// 3. timestamp에 찍힌 실제 배너 게시 시간.
const getImpressiontime = ({ connection, creatorId }) => new Promise((resolve, reject) => {
  const selectQuery = `
  SELECT  COUNT(*) AS impressiontime
  FROM campaignLog
  WHERE creatorId = ?
  AND type = 'CPM'
  AND date > ?
  `;

  doConnectionQuery({ connection, queryState: selectQuery, params: [creatorId, startDate] })
    .then((row) => {
      const { impressiontime } = row[0];
      resolve(impressiontime);
    })
    .catch((err) => {
      console.log(`${creatorId}의 노출시간을 구하는 과정`);
      console.log(err);
      resolve(0);
    });
});

const getTimeData = ({ connection, creatorId, creatorTwitchOriginalId }) => new Promise((resolve, reject) => {
    Promise.all([
      getAirtime({ connection, creatorId, creatorTwitchOriginalId }),
      getImpressiontime({ connection, creatorId })
    ])
      .then(([airtime, impressiontime]) => {
        let RIP = 0;
        if (airtime !== 0) {
          RIP = Number((impressiontime / airtime).toFixed(2));
          if (RIP >= 1) {
            RIP = 1;
          }
        } else {
          RIP = 0;
        }
        resolve(RIP);
      })
      .catch((error) => {
        console.log('streamDetail에서 데이터를 가져오는 과정');
        console.log(error);
        resolve(0);
      });
});

const getCheckDetail = ({ connection, creatorId }) => new Promise((resolve, reject) => {
  const selectQuery = `
  SELECT *
  FROM creatorDetail
  WHERE creatorId = ?
  `;

  doConnectionQuery({ connection, queryState: selectQuery, params: [creatorId] })
    .then((row) => {
      if (row.length > 0) {
        resolve(true);
      } else {
        resolve(false);
      }
    })
    .catch(() => {
      resolve(true);
    });
});


const getCreatorDetail = ({ connection, creatorId, creatorTwitchOriginalId }) => new Promise((resolve, reject) => {
  getViewerAirtime({ connection, creatorId, creatorTwitchOriginalId })
    .then((viewData) => {
      resolve();
      if (Object.entries(viewData).length === 0 && viewData.constructor === Object) {
        resolve();
        return;
      }
      const {
        airtime, viewer, peakview
      } = viewData;
      Promise.all([
        getClickPercent({ connection, creatorId }),
        getOpenHourPercent({ connection, creatorId, creatorTwitchOriginalId }),
        getContentsPercent({ connection, creatorId, creatorTwitchOriginalId }),
        getTimeData({ connection, creatorId, creatorTwitchOriginalId }),
      ])
        .then(([{ clickCount }, { openHour, timeGraphData }, { content, contentsGraphData }, rip]) => {
          if (viewer === 0 || airtime === 0 || rip == 0) {
            return [];
          }
          const impression =  Math.ceil(airtime * viewer * 6 * rip);
          const followers = 0;
          const ctr = Math.ceil(clickCount);
          const cost =  Math.ceil(viewer * 6 * 2 * rip);
          const timeJsonData = JSON.stringify({ data: timeGraphData });
          const contentsJsonData = JSON.stringify({ data: contentsGraphData });
          return [
            followers,
            ctr,
            airtime,
            viewer,
            impression,
            cost,
            content,
            openHour,
            timeJsonData,
            contentsJsonData,
            peakview,
            rip,
            creatorId
          ];
        })
        .then((params) => {
          if (params.length === 0) {
            getCheckDetail({ connection, creatorId })
            .then((check) => {
              const queryState = `
              DELETE 
              FROM creatorDetail
              WHERE creatorId = ?
              `;
              if(check)
              {
                doTransacQuery({ connection, queryState, params: [creatorId] })
                .then(() => {
                  resolve();
                })
                .catch((error) => {
                  console.log(`${creatorId}의 데이터를 저장하는 과정`);
                  console.log(error);
                  resolve();
                });
              }else {
                resolve();
              }
            });
          }else {
            getCheckDetail({ connection, creatorId })
              .then((check) => {
                const insertQuery = `
                INSERT INTO creatorDetail
                (followers, ctr, airtime, viewer, impression, cost, content, openHour, timeGraphData, contentsGraphData, peakview, rip, creatorId)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `;
                const updateQuery = `
                UPDATE creatorDetail
                SET 
                ctr = ?, 
                airtime = ?, 
                viewer = ?, 
                impression = ?,
                cost = ?,
                content = ?, 
                openHour = ?,
                timeGraphData = ?,
                contentsGraphData = ?, 
                peakview = ?, 
                rip = ?
                WHERE creatorId = ?
                `;

                const queryState = check ? updateQuery : insertQuery;
                const newParams = check ? params.slice(1) : params;

                doTransacQuery({ connection, queryState, params: newParams })
                  .then((res) => {
                    console.log(`[TWITCH] ${creatorId}(${creatorTwitchOriginalId}) creatorDetail INSERT OR UPDATE 완료`);
                    resolve();
                  })
                  .catch((error) => {
                    console.log(`${creatorId}의 데이터를 저장하는 과정`);
                    console.log(error);
                    resolve();
                  });
              });
          }
        });
    })
    .catch((error) => {
      console.log(error);
      resolve();
    });
});

const dividedGetDetail = (targetList) => new Promise((resolve, reject) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.log(err);
      reject(err);
    } else {
      Promise.all(
        targetList.map((creator) => getCreatorDetail({
          connection,
          creatorId: creator.creatorId,
          creatorTwitchOriginalId: creator.creatorTwitchOriginalId,
        }))
      ).then(() => {
        connection.release();
        setTimeout(() => {
          resolve();
        }, 10000);
      });
    }
  });
});

const getDoQueryCreatorList = () => new Promise((resolve, reject) => {
  const date = new Date();
  date.setDate(date.getDate() - 7);
  // 아프리카 추가 creatorId가 언제나 streamerId를 의미하지 않으므로, 크리에이터 데이터 추가 by hwasurr 21.01.14
  const selectQuery = `
  SELECT creatorId, creatorTwitchOriginalId
  FROM creatorInfo
  WHERE date < ?
  AND creatorContractionAgreement = 1
  `;
  doQuery(selectQuery, [date])
    .then((row) => {
      const creatorList = row.result;
      resolve(creatorList);
    })
    .catch((err) => {
      console.log('creator list를 가져오는 과정');
      console.log(err);
      resolve([]);
    });
});

const updateCalculation = async () => {
  const creatorList = await getDoQueryCreatorList();
  const turns = Math.ceil(creatorList.length / 30);
  const targetList = [];
  for (let i = 0; i < turns;) {
    const targets = creatorList.splice(0, 30);
    targetList.push(targets);
    i += 1;
  }
  return new Promise((resolve, reject) => {
    forEachPromise(targetList, dividedGetDetail)
      .then(() => { resolve(); });
  });
};

const setStartDate = () => {
  function pad(n, width) {
    n = n + '';
    return n.length >= width ? n : new Array(width - n.length + 1).join('0') + n;
  }
  const date = new Date();
  date.setMonth(date.getMonth()- 1);
  date.setDate(1);
  return `${date.getFullYear()}-${pad(date.getMonth(), 2)}-${pad(date.getDate(), 2)} 00:00:00`;
}

async function main() {
  console.log('=========================================');
  console.log('TWITCH creator detail data UPDATE START');
  console.log('=========================================');
  startDate = setStartDate();
 
  await updateCalculation(); 
  await updateFollower();
  console.log('=========================================');
  console.log('TWITCH creator detail data update complete!');
  console.log('=========================================');
}

module.exports = main;