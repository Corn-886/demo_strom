package pojo;

import java.io.Serializable;

/**文章实体
 * Created by Corn on 2017/4/3.
 */
public class ArticlePojo implements Serializable {
    private String title;
    private String datatime;
    private String content;
    private String keywords;//关键字
    private String note;//摘要

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDatatime() {
        return datatime;
    }

    public void setDatatime(String datatime) {
        this.datatime = datatime;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public ArticlePojo(String title, String datatime, String content, String keywords, String note) {
        this.title = title;
        this.datatime = datatime;
        this.content = content;
        this.keywords = keywords;
        this.note = note;
    }
    public ArticlePojo(){
        super();

    }

    @Override
    public String toString() {
        return "ArticlePojo{" +
                "title='" + title + '\'' +
                ", datatime='" + datatime + '\'' +
                ", content='" + content + '\'' +
                ", keywords='" + keywords + '\'' +
                ", note='" + note + '\'' +
                '}';
    }
}
