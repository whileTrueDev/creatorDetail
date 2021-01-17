const axios = require('axios');
const { doQuery, doTransacQuery } = require('../model/doQuery');
const pool = require('../model/connectionPool');
const forEachPromise = (items, fn) => items.reduce((promise, item) => promise.then(() => fn(item)), Promise.resolve());

/**
 * 지표 분석에서 팔로워 업데이트 작업 필요 타겟 BJ 목록을 불러옵니다.
 * 지표 분석된 모든 크리에이터.
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getDetaildoQueryList = () => new Promise((resolve, reject) => {
  const selectQuery = `
  SELECT CI.creatorId, afreecaId FROM creatorDetailAfreeca AS CDA
  JOIN creatorInfo AS CI ON CI.creatorId = CDA.creatorId
  `;
  doQuery(selectQuery)
    .then((row) => {
      // 타겟 유저 정보
      // [{ afreecaId: 'asdf', creatorId: 'asdf' }, { ... } ,... ]
      const creatorList = row.result;
      resolve(creatorList);
    })
    .catch((err) => {
      console.log('-------------------');
      console.error(err);
      console.error('ERROR - [updateFollower.afreeca.js] creator list를 가져오는 과정');
      console.log('-------------------');
      resolve([]);
    });
});

/**
 * 팔로워를 업데이트하는 함수
 * @param {object} param0 creatorId, connection, afreecaId
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const UpdateFollower = ({
  creatorId, connection, afreecaId
}) => new Promise((resolve, reject) => {
  const config = {
    headers: { 'User-Agent': 'Mozilla/5.0', }
  };
  const url = `https://bjapi.afreecatv.com/api/${afreecaId}/station`;
  axios.get(url, config)
    .then((res) => {
      resolve();
      const followers = res.data.station.upd.fan_cnt;
      const queryState = `
      UPDATE creatorDetailAfreeca
      SET followers = ?
      WHERE creatorId = ? 
      `;
      doTransacQuery({ connection, queryState, params: [followers, creatorId] })
        .then(() => {
          resolve();
        })
        .catch((error) => {
          console.log('-------------------');
          console.error(error);
          console.error(`ERROR - ${creatorId}의 데이터를 저장하는 과정`);
          console.log('-------------------');
          resolve();
        });
    })
    .catch((error) => {
      console.log('-------------------');
      console.error(error);
      console.error('ERROR - afreeca bjapi를 통한 구독자수 요청 실패');
      console.log('-------------------');
      resolve();
    });
});

/**
 * 분할된 팔로워 업데이트 작업을 실제 실행하는 함수
 * @param {list} targetList 타겟 creator 배열
 * @author chan
 */
const dividedGetFollower = (targetList) => new Promise((resolve, reject) => {
  pool.getConnection(async (err, connection) => {
    if (err) {
      console.log(err);
    } else {
      Promise.all(
        targetList.map((creator) => UpdateFollower({
          creatorId: creator.creatorId,
          afreecaId: creator.afreecaId,
          connection,
        }))
      ).then(() => {
        connection.release();
        setTimeout(() => {
          resolve();
        }, 60000);
      });
    }
  });
});

/**
 * 팔로워 업데이트 작업. 모든 크리에이터 중, 30명 씩 분할 실행 함수
 * @author chan
 */
const dividedUpdate = async () => {
  console.log('=========================================');
  console.log('Afreeca follower UPDATE START.');
  console.log('=========================================');
  const creatorList = await getDetaildoQueryList();
  
  const turns = Math.ceil(creatorList.length / 30);
  const targetList = [];
  for (let i = 0; i < turns;) {
    const targets = creatorList.splice(0, 30);
    targetList.push(targets);
    i += 1;
  }
  
  return new Promise((resolve, reject) => {
    forEachPromise(targetList, dividedGetFollower)
      .then(() => { resolve(); })
      .catch((err) => { reject(err); });
  });
};

module.exports = dividedUpdate;