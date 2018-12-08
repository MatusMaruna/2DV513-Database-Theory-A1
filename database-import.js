var mysql = require('mysql')
var fs = require('fs')
var lineReader = require('readline')

class DatabaseImport {
  constructor () {
    console.log('test')
  }
  initilize () {
    this.database = mysql.createConnection({ host: 'mm223fj.mynetgear.com', user: 'jb223pt', password: 'Avyq3Gm', database: 'Reddit_nocon', port: '20124' })
    this.subredditArray = []
    this.linkArray = []
    this.commentArray = []

    this.database.connect((err) => {
      if (err) {
        console.log('Could not connect to database!')
      } else {
        console.log('Connected!')
        this.readDataToArray()
      }
    })
  }
  async readDataToArray () {
    var reader = lineReader.createInterface({ input: fs.createReadStream('RC_2007-10.json') })
    var index = 0
    var currentDate = Date()
    console.log(currentDate)
    this.subredditArray = []
    this.linkArray = []
    this.commentArray = []
    await reader.on('line', (line) => {
      let data = JSON.parse(line)
      process.stdout.write('Insert ' + index++ + ' ' + this.subredditArray.length + ' ' + this.linkArray.length + ' ' + this.commentArray.length + '\r')
      this.subredditArray.push([ data.subreddit_id, data.subreddit ])
      this.linkArray.push([ data.link_id.split('_')[1], data.link_id, data.subreddit_id ])
      this.commentArray.push([ data.id, data.name, data.author, data.score, data.body, data.subreddit_id, data.parent_id, new Date(parseInt(data.created_utc) * 1000), data.link_id.split('_')[1] ])
    })
    reader.on('close', () => this.print())
  }
  print () {
    let chunkedSubreddit = this.chunk(this.subredditArray, 10000)
    let chunkedLink = this.chunk(this.linkArray, 10000)
    let chunkedComment = this.chunk(this.commentArray, 10000)
    for (let i = 0; i < chunkedSubreddit.length; i++) {
      this.database.query('INSERT IGNORE INTO Subreddit (id, name) VALUES ?', [chunkedSubreddit[i]])
    }
    for (let i = 0; i < chunkedLink.length; i++) {
      this.database.query('INSERT IGNORE INTO Link (id, name, subreddit_id) VALUES ?', [chunkedLink[i]])
    }
    for (let i = 0; i < chunkedComment.length; i++) {
      this.database.query('INSERT IGNORE INTO Comment (id, name, author, score, body, subreddit_id, parent_id, created_utc, link_id) VALUES ?', [chunkedComment[i]])
    }
  }

  chunk (arr, size) {
    var index = 0
    var arrayLength = arr.length
    var retArray = []

    for (index = 0; index < arrayLength; index += size) {
      let chunk = arr.slice(index, index + size)
      retArray.push(chunk)
    }

    return retArray
  }
}
module.exports = new DatabaseImport()