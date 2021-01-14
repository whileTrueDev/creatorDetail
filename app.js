require('dotenv').config(); 
console.log("크리에이터 상세정보 수집기 init!");
require('./javascripts/getCreatorDetailData')()
  .then(() => {
    require('./javascripts/getCreatorDetailData.afreeca')().then(() => {
      process.exit(0);
    })
  })