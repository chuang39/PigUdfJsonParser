/**
 * Created by khuang on 6/28/16.
 */

import java.io.IOException;

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.data.DataType;

import org.json.*;

public class JsonParser extends EvalFunc<Tuple> {

    protected final static TupleFactory tupleFactory = TupleFactory.getInstance();

    @Override
    public Schema outputSchema(Schema input) {
        Schema tupleSchema = new Schema();

        tupleSchema.add(new FieldSchema("isValid", DataType.CHARARRAY));
        tupleSchema.add(new FieldSchema("app", DataType.CHARARRAY));
        tupleSchema.add(new FieldSchema("pub", DataType.CHARARRAY));
        tupleSchema.add(new FieldSchema("w", DataType.INTEGER));
        tupleSchema.add(new FieldSchema("h", DataType.INTEGER));
        tupleSchema.add(new FieldSchema("bidid", DataType.CHARARRAY));
        tupleSchema.add(new FieldSchema("publisher", DataType.CHARARRAY));

        Schema s = new Schema(new FieldSchema(null, tupleSchema));
        return s;
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        Tuple t = tupleFactory.newTuple(7);

        if(input==null||input.size()==0||input.get(0)==null){
            t.set(0, "false");
            return t;
        }

        JSONObject obj = null;
        try{
            String str=(String) input.get(0);
            obj = new JSONObject(str);
        } catch (JSONException ex) {
            t.set(0, "false");
            return t;
        } catch (Exception ex) {
            String error = "Caught exception processing input row " + (String) input.get(0);
            throw new IOException(error,ex);
        }

        try {
            t.set(0, "true");
            JSONObject objBidReq = obj.getJSONObject("bidrequest");
            String pubName = null;

            if (objBidReq.has("app")) {
                JSONObject objApp = objBidReq.getJSONObject("app");
                if (objApp.has("name")) {
                    String appName = objApp.getString("name");
                    t.set(1, appName);
                }
                if (objApp.has("bundle")) {
                    String appBundle = objApp.getString("bundle");
                    t.set(2, appBundle);
                }

                if (objApp.has("publisher")) {
                    JSONObject objPublisher = objApp.getJSONObject("publisher");
                    if (objPublisher.has("name")) {
                        pubName = objPublisher.getString("name");
                        t.set(6, pubName);
                    }
                }

            } else if (objBidReq.has("site")) {
                JSONObject objSite = objBidReq.getJSONObject("site");
                if (objSite.has("name")) {
                    String siteName = objSite.getString("name");
                    t.set(1, siteName);
                    t.set(2, "SITE");
                }
            }

            JSONArray arrImp = objBidReq.getJSONArray("imp");
            JSONObject objImp0 = arrImp.getJSONObject(0);

            if (objImp0.has("banner")) {
                JSONObject objBanner = objImp0.getJSONObject("banner");

                int w = objBanner.getInt("w");
                int h = objBanner.getInt("h");
                t.set(3, w);
                t.set(4, h);
            } else if (objImp0.has("video")) {
                JSONObject objVideo = objImp0.getJSONObject("video");

                int w = objVideo.getInt("w");
                int h = objVideo.getInt("h");
                t.set(3, w);
                t.set(4, h);
            }

            String bidid = obj.getString("bid id");
            t.set(5, bidid);

            return t;
        }catch(Exception e){
            String error = "Caught exception processing input row " + (String) input.get(0);
            throw new IOException(error,e);
        }
    }
}