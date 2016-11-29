/*
 * Copyright 2015-2016 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package catalog;

import static catalog.CloudantUtil.createDocument;
import static catalog.CloudantUtil.getDocument;
import static catalog.CloudantUtil.setUp;
import static catalog.CloudantUtil.unsetUp;
import static common.TestUtils.SUCCESS_EXIT;
import static common.WskCli.Item.Action;
import static common.WskCli.Item.Package;
import static common.WskCli.Item.Rule;
import static common.WskCli.Item.Trigger;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.code.tempusfugit.concurrency.ParallelRunner;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import catalog.CloudantUtil.Credential;
import common.BlueProperties;
import common.Pair;
import common.TestUtils.RunResult;
import common.Util;
import common.WskCli;

/**
 * Tests for thumbnail action
 */
@RunWith(ParallelRunner.class)
public class ThumbnailDemoTests {

    private static final File thumbnailCode = BlueProperties.getFileRelativeToWhiskHome("catalog/actions/thumbnail/thumbnail.js");

    // thumbnail uses this to interact with cloudant
    private static final String CLOUDANT_PACKAGE = "/whisk.system/cloudant";
    private static final String CLOUDANT_FEED = "changes";
    private static final String THUMBNAIL_PROP_FILE = "tests/dat/cloudant.thumbnail.properties";
    private static final String IMAGE_PROP_FILE = "tests/dat/cloudant.image.properties";
    private static final Credential thumbnailCred = Credential.makeFromPropertyFile(THUMBNAIL_PROP_FILE);
    private static final Credential imageCred = Credential.makeFromPropertyFile(IMAGE_PROP_FILE);
    public final static String thumbnailImage = "perryibm/thumbnail";

    private static int waitTime = 60;
    private static final WskCli wsk = new WskCli();

    @BeforeClass
    public static void setupDatabases() throws Exception {
        setUp(thumbnailCred);
        setUp(imageCred);
    }

    @AfterClass
    public static void teardownDatabases() throws Exception {
        unsetUp(thumbnailCred);
        unsetUp(imageCred);
    }

    private static String createImageDoc(File file) throws Exception {
        long fileSize = file.length();
        byte[] inputData = Util.readFile64(file);
        String imagePayload = new String(inputData, "UTF-8");
        // System.out.println("imagePayload.size = " + imagePayload.length());
        return "{ \"size\" : \"" + fileSize + "\", \"data\" : \"" + imagePayload + "\"  }";
    }

    // Write the binary file in base64 encoded form into the image database, returning the document id.
    private static String writeImageToDatabase(File file) throws Exception {
        long start = System.currentTimeMillis();
        JsonObject createResponse = createDocument(imageCred, createImageDoc(file));
        assertTrue("Failed to create document. ", createResponse.has("id"));
        String imageId = createResponse.get("id").getAsString();
        long fileSize = file.length();
        System.out.format("Image of size %d written in %f seconds.  docId = %s\n",
                          fileSize, (System.currentTimeMillis() - start) / 1000.0, imageId);
        return imageId;
    }

    /**
     * register a cloudant trigger with the service
     *
     * @throws UnsupportedEncodingException
     */
    private static void registerTriggerWithPackage(Credential credential, String specificCloudantPackage,
                                                  String trigger, boolean includeDoc) throws Exception {
        WskCli wsk = new WskCli();
        wsk.sanitize(Package, specificCloudantPackage);

        // cloudant package instance
        @SuppressWarnings("serial")
        Map<String, String> cloudantInstanceParams = new HashMap<String, String>() {
            {
                put("host", credential.host());
                put("username", credential.user);
                put("password", credential.password);
                put("includeDoc", includeDoc ? "true" : "false");
            }
        };
        wsk.bindPackage(SUCCESS_EXIT, CLOUDANT_PACKAGE, specificCloudantPackage, cloudantInstanceParams);

        String result = wsk.cli("trigger", "create", trigger, "--auth", wsk.authKey, "--feed", specificCloudantPackage + "/" + CLOUDANT_FEED, "-p", "dbname", credential.dbname).stdout;
        assertTrue("could create trigger and invoke feed", result.contains("ok"));
        System.out.println("invoked feed to create trigger and got: " + result);

        System.out.println("Sleeping 20 seconds...");
        Thread.sleep(20 * 1000);  // wait 20 seconds for the trigger to register in cloudant
    }

    /*
     * Create a thumbnail action and test it with an image.
     * Test side:  Create a cloudant doc and its id (along with its database and an output database) to the action.
     * Whisk side: Fetch doc, write file locally, run image magick to get thumbnail, read file locally, write to cloudant, return doc id
     * Test side:  Fetch cloudant doc with result id.  Write file locally.  Read original file and new file as images and check sizes.
     */
    @Test(timeout=120*1000)
    public void thumbnailAction() throws Exception {

        // See issue 1054 (closed but not solved - crosslinked) for why actionName not reused
        long start = System.currentTimeMillis();
        String actionName = "thumbnailAction" + (start % 1000);

        try {
            wsk.sanitize(Action, actionName);
            String file = thumbnailCode.getAbsolutePath();
            wsk.createAction(actionName, file);
            System.out.println("Created action " + actionName + " with " + file);
            File inFile = BlueProperties.getFileRelativeToWhiskHome("tests/dat/images/VeilNebula.png");
            thumbnailInner(actionName, inFile);
        } finally {
            wsk.sanitize(Action, actionName);
        }
    }

    /*
     * Create a thumbnail package with a single action which generates a thumbnail.
     * Use a different test file than thumbnailAction.
     */
    @Test(timeout=120*1000)
    public void thumbnailPackage() throws Exception {

        // See issue 1054 (closed but not solved - crosslinked) for why actionName not reused
        long start = System.currentTimeMillis();
        String packageName = "thumbnail" + (start % 1000);
        String packageActionName = packageName + "/make" + (start % 1000);

        try {
            wsk.sanitize(Package, packageName);
            wsk.createPackage(packageName, null);
            wsk.createAction(packageActionName, thumbnailCode.getAbsolutePath());
            File inFile = BlueProperties.getFileRelativeToWhiskHome("tests/dat/images/EarthHeart.png");
            thumbnailInner(packageActionName, inFile);
        } finally {
            wsk.sanitize(Action, packageActionName);
            wsk.sanitize(Package, packageName);
        }

    }

    private void thumbnailInner(String actionName, File inFile) throws Exception {

        // Retrieve input image from data directory
        String path = inFile.getAbsolutePath();
        String last = path.substring(path.lastIndexOf('/') + 1);
        File outFile = File.createTempFile("thumbnail-" + last, ".png");
        String imageId = writeImageToDatabase(inFile);

        // Run thumbnail in cloudant mode - set up the payload
        //JsonObject actionArg = new JsonObject();
        Map<String, String> params = new HashMap<String, String>();
        params.put("thumbnailCred", thumbnailCred.toJson().toString());
        params.put("imageCred", imageCred.toJson().toString());
        params.put("imageDocId", imageId);

        // Run the action blockingly and extract the doc id from the result
        Pair<String,String> resultPair = wsk.invokeBlocking(actionName, params);
        String activationId = resultPair.fst;
        String whiskResponseStr = resultPair.snd;
        System.out.println("Id:     " + activationId);
        System.out.println("Result: " + whiskResponseStr);
        JsonObject whiskResponse = new JsonParser().parse(whiskResponseStr).getAsJsonObject();
        String resultDocId = whiskResponse.get("result").getAsJsonObject().get("id").getAsString();
        System.out.println("Doc Id: " + resultDocId);

        // Check the logs in case something bad happened
        String expected = "Thumbnail complete";
        String log = wsk.logsForActivationContainGet(activationId, expected, waitTime);
        if (log == null) {
            System.out.println("Did not find " + expected + " in activation " + activationId);
            RunResult logRes = wsk.getLogsForActivation(activationId);
            System.out.println("Log: " + logRes.stdout);
            assertFalse("Missing keyword " + expected, true);
        } else {
            System.out.println("Found " + expected + " in activation " + activationId);
            System.out.println("Log: " + log + "\n");
        }

        // Fetch document from cloudant and convert to binary.  Write to results dir.
        JsonObject getResponse = getDocument(thumbnailCred, resultDocId);
        byte[] base64Data = getResponse.get("thumbnail").getAsString().getBytes();
        Util.writeFile64(outFile, base64Data);

        // Check that it's a legit thumbnail image by reading it back and checking it is smaller
        try {
            BufferedImage inImage = ImageIO.read(inFile);
            int inWidth = inImage.getWidth();
            int inHeight = inImage.getWidth();
            BufferedImage outImage = ImageIO.read(outFile);
            int outWidth = outImage.getWidth();
            int outHeight = outImage.getWidth();
            System.out.format("Size check:  %d X %d  ->  %d X %d\n", inWidth, inHeight, outWidth, outHeight);
            assertTrue("Illegal dimensions", inWidth > 0 && inHeight > 0 && outWidth > 0 && outHeight > 0);
                assertTrue("Thumbnail not smaller", inWidth > outWidth && inHeight > outHeight);
        } catch (IOException e) {
            assertFalse("Failed to read in original or thumbnail thumbnail", true);
        }
    }


    public static String createDoc(String s) {
        String QUOTE = "\"";
        String body = "{";
        body += QUOTE + "message" + QUOTE + ":" + QUOTE + s + QUOTE;
        body += "}";
        return body;
    }

    /*
     * Use cloudant trigger to drive thumbnail
     */
    @Test(timeout=120*1000)
    public void thumbnailCloudant() throws Exception {

        // This is specific to how tests are done here
        long suffix = System.currentTimeMillis() % 1000;
        String MY_TRIGGER = "thumbnail_cloudant_trigger" + suffix;
        String MY_ACTION = "thumbnail_cloudant_action" + suffix;
        String MY_RULE = "thumbnail_cloudant_rule" + suffix;
        String MY_CLOUDANT_PACKAGE = "thumbnail_cloudant" + suffix;
        String actionFile = BlueProperties.getFileRelativeToWhiskHome("catalog/actions/thumbnail/thumbnail.js").getAbsolutePath();
        int DELAY = 60;

        WskCli wsk = new WskCli();
        wsk.sanitize(Action, MY_ACTION);
        wsk.sanitize(Trigger, MY_TRIGGER);
        wsk.sanitize(Rule, MY_RULE);

        try {
            registerTriggerWithPackage(imageCred, MY_CLOUDANT_PACKAGE, MY_TRIGGER, false);

            // We flatten the credential because parameters cannot support nested objects.
            @SuppressWarnings("serial")
            HashMap<String, String> boundParams = new HashMap<String, String>() {{
                put("imageUser", imageCred.user);
                put("imagePassword", imageCred.password);
                put("imageDbname", imageCred.dbname);
                put("thumbnailUser", thumbnailCred.user);
                put("thumbnailPassword", thumbnailCred.password);
                put("thumbnailDbname", thumbnailCred.dbname);
            }};

            wsk.createAction(MY_ACTION, actionFile, boundParams);
            wsk.createRule(MY_RULE, MY_TRIGGER, MY_ACTION);

            File inFile = BlueProperties.getFileRelativeToWhiskHome("tests/dat/images/VeilNebula.png");

            // create document in db...
            JsonObject createResponse = createDocument(imageCred, createImageDoc(inFile));  // change to createImageDoc and the test will fail
            assertTrue("Failed to create document. ", createResponse.has("id"));
            // ...and verify that it exists
            String docId = createResponse.get("id").getAsString();
            JsonObject getResponse = getDocument(imageCred, docId);
            assertTrue("New document isn't in the database. " + getResponse, getResponse.toString().contains("size"));

            // wait for action activation
            String expected = "Result document id = ";
            String log = wsk.firstLogsForActionContainGet(MY_ACTION, expected, 0L, DELAY);
            if (log == null) {
                List<String> allLogs = wsk.getLogsForAction(MY_ACTION, 0L);
                for (String s : allLogs)
                    System.out.println("LOG: " + s);
            }
            assertTrue("log msg '" + expected + "' not found", log != null);
        } finally {
            wsk.sanitize(Rule, MY_RULE);
            wsk.sanitize(Action, MY_ACTION);
            wsk.sanitize(Trigger, MY_TRIGGER);
            wsk.sanitize(Package, MY_CLOUDANT_PACKAGE);
        }
    }

}
