export function createMessagingFeature(deps) {
  const {
    db,
    functions,
    getCurrentUser,
    getCurrentProfile,
    getCurrentView,
    setCurrentView,
    cleanupListeners,
    showLogin,
    showMessage,
    trackJourneyEvent,
    addNotification,
    isUserHidden,
    ensureSparkBotConversation,
    collection,
    query,
    where,
    orderBy,
    onSnapshot,
    getDocs,
    getDoc,
    addDoc,
    updateDoc,
    doc,
    arrayRemove,
    arrayUnion,
    serverTimestamp,
    httpsCallable,
    escapeHtml,
    formatTime,
    renderTextWithMentions,
    PLATFORM_GUIDE_USER_ID,
    PLATFORM_GUIDE_NAME,
    SPARK_BOT_ID,
    SPARK_BOT_NAME,
    futureSelfName,
  } = deps;

  let messagesListener = null;
  let chatListener = null;
  let activeConvId = null;
  let pendingConvOpen = null;

  function cleanupMessagingListeners() {
    if (chatListener) {
      chatListener();
      chatListener = null;
    }
    if (messagesListener) {
      messagesListener();
      messagesListener = null;
    }
    activeConvId = null;
  }

  function getCurrentUsername() {
    const currentUser = getCurrentUser();
    return currentUser?.email?.split('@')[0] || 'member';
  }

  function getConversationDisplayName(conv) {
    const currentUser = getCurrentUser();
    if (conv.botType === 'platformGuide') return PLATFORM_GUIDE_NAME;
    if (conv.botType === 'sparkBot') return SPARK_BOT_NAME;
    if (conv.botType === 'futureSelf') return futureSelfName();
    return conv.participantNames
      ? conv.participantNames.find((name) => name !== currentUser.email.split('@')[0]) || 'Unknown'
      : 'Unknown';
  }

  async function showMessages() {
    const currentUser = getCurrentUser();
    if (!currentUser) {
      showLogin();
      return;
    }

    setCurrentView('messages');
    activeConvId = null;
    cleanupListeners();
    const appEl = document.getElementById('app');
    appEl.innerHTML = '<div class="loading">Loading messages...</div>';

    await ensurePlatformGuideConversation();
    await ensureSparkBotConversation();

    const convQuery = query(
      collection(db, 'conversations'),
      where('participants', 'array-contains', currentUser.uid),
      orderBy('lastMessageAt', 'desc'),
    );

    messagesListener = onSnapshot(convQuery, (snapshot) => {
      const conversations = [];
      snapshot.forEach((d) => conversations.push({ id: d.id, ...d.data() }));

      let convListHtml = '<h3>Conversations</h3>';
      if (conversations.length === 0) {
        convListHtml += '<div style="padding:15px;color:var(--gray);font-size:13px;">No conversations yet. Visit a member\'s profile to start one.</div>';
      }

      for (const conv of conversations) {
        const otherParticipantId = (conv.participants || []).find((id) => id !== currentUser.uid);
        if (otherParticipantId && otherParticipantId !== PLATFORM_GUIDE_USER_ID && isUserHidden(otherParticipantId)) {
          continue;
        }

        const hasUnread = conv.unreadBy && conv.unreadBy.includes(currentUser.uid);
        convListHtml += `
          <div class="conv-item ${conv.id === activeConvId ? 'active' : ''}" onclick="window.openConversation('${conv.id}')">
            <div class="conv-name">${escapeHtml(getConversationDisplayName(conv))} ${hasUnread ? '<span class="unread-dot"></span>' : ''}</div>
            <div class="conv-preview">${escapeHtml(conv.lastMessage || '')}</div>
            <div class="conv-time">${formatTime(conv.lastMessageAt)}</div>
          </div>
        `;
      }

      const existingList = document.querySelector('.conversations-list');
      if (existingList && activeConvId) {
        existingList.innerHTML = convListHtml;
      } else {
        appEl.innerHTML = `
          <div class="messaging-layout">
            <div class="conversations-list">${convListHtml}</div>
            <div class="chat-area">
              <div class="chat-empty">Select a conversation</div>
            </div>
          </div>
        `;
      }

      if (pendingConvOpen) {
        const convToOpen = pendingConvOpen;
        pendingConvOpen = null;
        openConversation(convToOpen);
      }
    });
  }

  async function openGuideConversation() {
    const currentUser = getCurrentUser();
    if (!currentUser) {
      showLogin();
      return;
    }

    const conversationId = await ensurePlatformGuideConversation();
    await trackJourneyEvent('guide_opened', { source: getCurrentView() || 'unknown' });
    pendingConvOpen = conversationId;
    await showMessages();
  }

  async function openConversation(convId) {
    const currentUser = getCurrentUser();
    if (!currentUser) return;

    if (chatListener) chatListener();
    activeConvId = convId;

    const convRef = doc(db, 'conversations', convId);
    await updateDoc(convRef, { unreadBy: arrayRemove(currentUser.uid) });

    const chatArea = document.querySelector('.chat-area');
    if (!chatArea) return;

    const convSnap = await getDoc(convRef);
    if (!convSnap.exists()) {
      showMessage('Conversation not found', 'error');
      return;
    }

    const convData = convSnap.data();
    const otherName = getConversationDisplayName(convData);

    chatArea.innerHTML = `
      <div class="chat-header">${escapeHtml(otherName)}</div>
      <div class="chat-messages" id="chatMessages"><div class="loading">Loading...</div></div>
      <div class="chat-input">
        <input type="text" id="msgInput" placeholder="Type a message..." onkeydown="if(event.key==='Enter'){window.sendMessage('${convId}');event.preventDefault();}">
        <button onclick="window.sendMessage('${convId}')">Send</button>
      </div>
    `;

    document.querySelectorAll('.conv-item').forEach((el) => el.classList.remove('active'));
    document.querySelectorAll('.conv-item').forEach((el) => {
      if (el.getAttribute('onclick')?.includes(convId)) el.classList.add('active');
    });

    const msgsQuery = query(collection(db, 'conversations', convId, 'messages'), orderBy('timestamp', 'asc'));
    chatListener = onSnapshot(msgsQuery, (snapshot) => {
      const messages = [];
      snapshot.forEach((d) => messages.push({ id: d.id, ...d.data() }));

      const chatMsgs = document.getElementById('chatMessages');
      if (!chatMsgs) return;

      if (messages.length === 0) {
        chatMsgs.innerHTML = '<div class="chat-empty">No messages yet. Say hello!</div>';
        return;
      }

      let html = '';
      for (const msg of messages) {
        const isMine = msg.authorId === currentUser.uid;
        html += `
          <div class="chat-msg ${isMine ? 'mine' : ''}">
            <div class="msg-author">${isMine ? 'you' : escapeHtml(msg.author)}</div>
            <div class="msg-bubble">${renderTextWithMentions(msg.content || '')}</div>
            <div class="msg-time">${formatTime(msg.timestamp)}</div>
          </div>
        `;
      }
      chatMsgs.innerHTML = html;
      chatMsgs.scrollTop = chatMsgs.scrollHeight;
    });
  }

  async function sendMessage(convId) {
    const currentUser = getCurrentUser();
    if (!currentUser) return;

    const input = document.getElementById('msgInput');
    const text = input.value.trim();
    if (!text) return;
    input.value = '';

    try {
      const username = getCurrentUsername();
      await addDoc(collection(db, 'conversations', convId, 'messages'), {
        content: text,
        author: username,
        authorId: currentUser.uid,
        timestamp: serverTimestamp(),
      });

      const convSnap = await getDoc(doc(db, 'conversations', convId));
      if (!convSnap.exists()) return;

      const convData = convSnap.data();
      const otherId = convData.participants.find((p) => p !== currentUser.uid);
      const isPlatformGuide = convData.botType === 'platformGuide' || otherId === PLATFORM_GUIDE_USER_ID;

      await updateDoc(doc(db, 'conversations', convId), {
        lastMessage: text,
        lastMessageAt: serverTimestamp(),
        unreadBy: isPlatformGuide ? arrayRemove(currentUser.uid) : arrayUnion(otherId),
      });

      if (!isPlatformGuide) {
        await addNotification(otherId, 'message', `${username} sent you a message`, null);
      }
    } catch (error) {
      console.error('Error sending message:', error);
      showMessage('Error sending message', 'error');
    }
  }

  async function findPlatformGuideConversationId() {
    const currentUser = getCurrentUser();
    if (!currentUser) return null;

    const snap = await getDocs(query(collection(db, 'conversations'), where('participants', 'array-contains', currentUser.uid)));
    let conversationId = null;
    snap.forEach((d) => {
      const data = d.data();
      if (data.botType === 'platformGuide' || (data.participants || []).includes(PLATFORM_GUIDE_USER_ID)) {
        conversationId = d.id;
      }
    });
    return conversationId;
  }

  async function createLocalPlatformGuideConversation() {
    const currentUser = getCurrentUser();
    const currentProfile = getCurrentProfile();
    const username = currentProfile?.username || currentUser.email.split('@')[0] || 'member';
    const welcomeMessage = `Hi ${username}, I am ${PLATFORM_GUIDE_NAME}. JourneyHub is built to help builders find better signal: the right resources, communities, people, and next steps. Tell me what you are trying to grow, and I will help you find the best place to start.`;

    const convRef = await addDoc(collection(db, 'conversations'), {
      participants: [currentUser.uid, PLATFORM_GUIDE_USER_ID],
      participantNames: [username, PLATFORM_GUIDE_NAME],
      lastMessage: welcomeMessage,
      lastMessageAt: serverTimestamp(),
      unreadBy: [currentUser.uid],
      botType: 'platformGuide',
      botUserId: PLATFORM_GUIDE_USER_ID,
      createdAt: serverTimestamp(),
    });

    await addDoc(collection(db, 'conversations', convRef.id, 'messages'), {
      content: welcomeMessage,
      author: PLATFORM_GUIDE_NAME,
      authorId: PLATFORM_GUIDE_USER_ID,
      timestamp: serverTimestamp(),
      botType: 'platformGuide',
    });

    await addNotification(currentUser.uid, 'message', `${PLATFORM_GUIDE_NAME} sent you a welcome message`, null);
    return convRef.id;
  }

  async function ensurePlatformGuideConversation() {
    const currentUser = getCurrentUser();
    if (!currentUser) return null;

    const existingConversationId = await findPlatformGuideConversationId();
    if (existingConversationId) return existingConversationId;

    try {
      const fn = httpsCallable(functions, 'ensurePlatformGuideConversation');
      const result = await fn();
      const createdConversationId = result.data?.conversationId || null;
      if (createdConversationId) return createdConversationId;
    } catch (error) {
      console.error('Error ensuring platform guide conversation:', error);
    }

    const fallbackConversationId = await findPlatformGuideConversationId();
    if (fallbackConversationId) return fallbackConversationId;

    try {
      return await createLocalPlatformGuideConversation();
    } catch (fallbackError) {
      console.error('Error creating local platform guide conversation:', fallbackError);
      return null;
    }
  }

  async function startConversation(otherUserId, otherUsername) {
    const currentUser = getCurrentUser();
    if (!currentUser) return;

    const q = query(collection(db, 'conversations'), where('participants', 'array-contains', currentUser.uid));
    const snap = await getDocs(q);
    let existingConvId = null;
    snap.forEach((d) => {
      if (d.data().participants.includes(otherUserId)) {
        existingConvId = d.id;
      }
    });

    if (existingConvId) {
      pendingConvOpen = existingConvId;
      await showMessages();
      return;
    }

    const convRef = await addDoc(collection(db, 'conversations'), {
      participants: [currentUser.uid, otherUserId],
      participantNames: [getCurrentUsername(), otherUsername],
      lastMessage: '',
      lastMessageAt: serverTimestamp(),
      unreadBy: [],
    });

    pendingConvOpen = convRef.id;
    await showMessages();
  }

  async function openSparkBot() {
    const currentUser = getCurrentUser();
    if (!currentUser) {
      showLogin();
      return;
    }

    try {
      const fn = httpsCallable(functions, 'ensureSparkBotConversation');
      const result = await fn({});
      const convId = result.data.convId;
      await showMessages();
      pendingConvOpen = convId;
    } catch (e) {
      console.error('Error opening spark bot:', e);
      showMessage('Error opening Spark Bot', 'error');
    }
  }

  return {
    cleanupMessagingListeners,
    ensurePlatformGuideConversation,
    openConversation,
    openGuideConversation,
    openSparkBot,
    sendMessage,
    showMessages,
    startConversation,
  };
}
